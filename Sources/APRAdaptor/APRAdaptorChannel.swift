//
//  APRAdaptorChannel.swift
//  ZeeQL
//
//  Created by Helge Hess on 23/02/17.
//  Copyright © 2017 ZeeZide GmbH. All rights reserved.
//

import struct Foundation.TimeInterval
import struct Foundation.Date
import ZeeQL
import CAPR
import CAPRUtil

open class APRAdaptorChannel : AdaptorChannel {
  
  open  var log                      : ZeeQLLogger
  final public let expressionFactory : SQLExpressionFactory
  final public let pool   : OpaquePointer // UnsafePointer<apr_pool_t>
  final public let con    : OpaquePointer // UnsafePointer<apr_dbd_t>
  final public let driver : OpaquePointer // UnsafePointer<apr_dbd_driver_t>
  final let ownsPool       : Bool
  final let ownsConnection : Bool
  
  final let logSQL = true
  
  // MARK: - Statistics
  
  public struct Statistics {
    let openedAt        = Date()
    var txBeginCount    = 0
    var txCommitCount   = 0
    var txRollbackCount = 0
    
    var age : TimeInterval { return Date().timeIntervalSince(openedAt) }
  }
  public var statistics = Statistics()
  
  
  // MARK: - Constructor
  
  public init(adaptor    : Adaptor,
              pool       : OpaquePointer, // UnsafePointer<apr_pool_t>
              driver     : OpaquePointer, // UnsafePointer<apr_dbd_driver_t>
              connection : OpaquePointer, // UnsafePointer<apr_dbd_t>
              ownsPool   : Bool = true, ownsConnection: Bool = true) throws
  {
    self.expressionFactory = adaptor.expressionFactory
    self.pool           = pool
    self.con            = connection
    self.tx             = nil
    self.log            = adaptor.log
    
    self.ownsPool       = ownsPool
    self.ownsConnection = ownsConnection
    
    self.driver         = driver
  }
  
  deinit {
    if ownsPool { apr_pool_destroy(pool) }
  }
  
  func close() {
    if ownsConnection {
      apr_dbd_close(driver, con)
    }
  }
  
  
  // MARK: - Transactions

  final public var tx : OpaquePointer? // UnsafePointer<apr_dbd_transaction_t>

  public var isTransactionInProgress : Bool { return tx != nil }
  
  public func begin() throws {
    guard tx == nil else { throw AdaptorChannelError.TransactionInProgress }
    
    let rc = apr_dbd_transaction_start(driver, pool, con, &tx)
    guard rc == 0 else { throw APRDBDError(rc, message: message(for: rc)) }
    
    statistics.txBeginCount += 1
  }
  
  public func commit() throws {
    try _endTransaction(mode: APR_DBD_TRANSACTION_COMMIT)
    statistics.txCommitCount += 1
  }
  public func rollback() throws {
    try _endTransaction(mode: APR_DBD_TRANSACTION_ROLLBACK)
    statistics.txRollbackCount += 1
  }
  
  func _endTransaction(mode: Int32) throws {
    guard tx != nil else { return } // no TX open
    
    let activeMode = apr_dbd_transaction_mode_set(driver, tx, mode)
    guard activeMode == mode else {
      // TBD: raise what?
      let _ = apr_dbd_transaction_end(driver, pool, tx)
      tx = nil
      return
    }
    
    let rc = apr_dbd_transaction_end(driver, pool, tx)
    tx = nil
    guard rc == 0 else {
      throw APRDBDError(rc, message: message(for: rc))
    }
  }
  
  
  // MARK: - SQL w/o result
  
  @discardableResult
  public func performSQL(_ sql: String) throws -> Int {
    var affected : Int32 = 0
    let rc = apr_dbd_query(driver, con, &affected, sql)
    guard rc == APR_SUCCESS else {
      throw APRDBDError(rc, message: message(for: rc))
    }
    return Int(affected)
  }
  
  
  // MARK: - Selects
  
  func fetchRows(_ res      : OpaquePointer?,
                 _ optAttrs : [ Attribute ]? = nil,
                 cb         : ( AdaptorRecord ) throws -> Void) throws
  {
    var row        : OpaquePointer? = nil // UnsafePointer<apr_dbd_row_t>
      // The row needs to live outside of the loop, the PG driver is reusing it
      // as the cursor.
    
    var schema     : AdaptorRecordSchema?
      // assumes uniform results, which should be so
    
    let rowNextRow : Int32 = -1  // constant
    
    if let attrs = optAttrs {
      schema = AdaptorRecordSchemaWithAttributes(attrs)
    }
    
    while true {
      // TBD: create a new pool for each row to keep alloc low?
      let rc = apr_dbd_get_row(driver, pool, res, &row, rowNextRow)

      // How to distinguish between EOF and an actual error?!
      guard rc == 0 else {
        let msg = message(for: rc)
        if rc == -1 {
          if msg == ""             { break } // pgsql - no actual error, EOF
          if msg == "not an error" { break } // SQLite3, EOF
        }
        throw APRDBDError(rc, message: msg)
      }
      
      let colCount = apr_dbd_num_cols(driver, res)
      
      // build schema if no attributes have been provided
      if schema == nil {
        // TBD: Do we want to build attributes? Probably not, too expensive for
        //      simple stuff.
        var names = [ String ]()
        names.reserveCapacity(Int(colCount))
        
        for colIdx in 0..<colCount {
          if let attrs = optAttrs, Int(colIdx) < attrs.count,
             let col = attrs[Int(colIdx)].columnName
          {
            names.append(col)
          }
          else if let name = apr_dbd_get_name(driver, res, colIdx) {
            names.append(String(cString: name))
          }
          else {
            names.append("col[\(colIdx)]")
          }
        }
        schema = AdaptorRecordSchemaWithNames(names)
      }
      
      var values = [ Any? ]()
      values.reserveCapacity(Int(colCount))
      
      for colIdx in 0..<colCount {
        let attr : Attribute?
        if let attrs = optAttrs, Int(colIdx) < attrs.count {
          attr = attrs[Int(colIdx)]
        }
        else {
          attr = nil
        }
        
        values.append(valueIn(row: row, column: colIdx, attribute: attr))
      }
      
      let record = AdaptorRecord(schema: schema!, values: values)
      try cb(record)
    }
  }
  
  func aprRawGet<T>(_ value: inout T,
                    _ row: OpaquePointer?, _ column: Int32,
                    _ aprType: apr_dbd_type_e) -> T?
  {
    let status = apr_dbd_datum_get(driver, row, column, aprType, &value)
    guard status != APR_ENOENT else { return nil }
    return value
  }
  
  open func valueIn(row: OpaquePointer?, column: Int32, attribute: Attribute?)
            -> Any?
  {
    // Note: I think there is no APR function to get the 'native' type of a
    //       column. Which is why we need to fallback to String if we got
    //       no attribute.
    
    if let aprType = attribute?.aprTypeForExternalType
                  ?? attribute?.aprTypeForValueType,
           aprType != APR_DBD_TYPE_NONE
    {
      // NULL is status = APR_ENOENT
      
      // let status = apr_dbd_datum_get(driver, row, column, aprType, &data)
      
      switch aprType {
        case APR_DBD_TYPE_NULL:
          return nil
        
        case APR_DBD_TYPE_INT:
          var value : CInt = 0
          return aprRawGet(&value, row, column, aprType)
        case APR_DBD_TYPE_UINT:
          var value : CUnsignedInt = 0
          return aprRawGet(&value, row, column, aprType)
        
        case APR_DBD_TYPE_TINY:
          var value : CChar = 0
          return aprRawGet(&value, row, column, aprType)
        case APR_DBD_TYPE_UTINY:
          var value : CUnsignedChar = 0
          return aprRawGet(&value, row, column, aprType)
        
        case APR_DBD_TYPE_SHORT:
          var value : CShort = 0
          return aprRawGet(&value, row, column, aprType)
        case APR_DBD_TYPE_USHORT:
          var value : CUnsignedShort = 0
          return aprRawGet(&value, row, column, aprType)
        
        case APR_DBD_TYPE_LONG:
          var value : CLong = 0
          return aprRawGet(&value, row, column, aprType)
        case APR_DBD_TYPE_ULONG:
          var value : CUnsignedLong = 0
          return aprRawGet(&value, row, column, aprType)
        
        case APR_DBD_TYPE_LONGLONG:
          var value : CLongLong = 0
          return aprRawGet(&value, row, column, aprType)
        case APR_DBD_TYPE_ULONGLONG:
          var value : CUnsignedLongLong = 0
          return aprRawGet(&value, row, column, aprType)
        
        case APR_DBD_TYPE_DOUBLE:
          var value : CDouble = 0
          let status = apr_dbd_datum_get(driver, row, column, aprType, &value)
          guard status != APR_ENOENT else { return nil }
          return value
        
        case APR_DBD_TYPE_FLOAT:
          var value : CFloat = 0
          let status = apr_dbd_datum_get(driver, row, column, aprType, &value)
          guard status != APR_ENOENT else { return nil }
          return value
        
        case APR_DBD_TYPE_STRING, APR_DBD_TYPE_TEXT:
          // Note: apr_dbd_get_entry() doesn't work properly for NULL w/ PG
          var value  : UnsafeMutablePointer<CChar>? = nil
          let status = apr_dbd_datum_get(driver, row, column, aprType, &value)
          guard status != APR_ENOENT else { return nil }
          assert(value != nil)
          guard let cstr = value else { return nil }
          return String(cString: cstr)
        
        // TODO: Convert to Date etc - DB specific
        case APR_DBD_TYPE_ZTIMESTAMP:
          break
        case APR_DBD_TYPE_TIMESTAMP:
          break
        case APR_DBD_TYPE_DATETIME:
          break
        case APR_DBD_TYPE_TIME:
          break
        case APR_DBD_TYPE_DATE:
          break
        
        case APR_DBD_TYPE_BLOB, APR_DBD_TYPE_CLOB:
          // data is an apr_bucket_brigade
          break
        
        default:
          break
      }
    }
    
    // Note: apr_dbd_get_entry() doesn't work properly for NULL w/ PG
    var value  : UnsafeMutablePointer<CChar>? = nil
    let status = apr_dbd_datum_get(driver, row, column, APR_DBD_TYPE_STRING,
                                   &value)
    guard status != APR_ENOENT else { return nil }
    assert(value != nil)
    if let cstr = value , status == APR_SUCCESS {
      return String(cString: cstr)
    }
    
    // Note: This returns "" for NULL w/ the APR PostgreSQL adaptor. (instead of
    //       nil). This is because the PG adaptor just calls PQgetvalue() which
    //       returns an empty string.
    guard let svalue = apr_dbd_get_entry(driver, row, column)
     else { return nil }
    return String(cString: svalue)
  }
  
  
  open func querySQL(_ sql: String, _ optAttrs : [ Attribute ]? = nil,
                     cb: ( AdaptorRecord ) throws -> Void) throws
  {
    if logSQL { log.log("SQL: \(sql)") }
    
    // pool for fetch
    var pool : OpaquePointer? = nil
    let status = apr_pool_create_ex(&pool, self.pool, nil, nil)
    guard status == APR_SUCCESS && pool != nil else { throw APRError(status) }
    defer { if let p = pool { apr_pool_destroy(p) } }
    
    // run DBD query
    var res : OpaquePointer? = nil // UnsafePointer<apr_dbd_results_t>
    let rc = apr_dbd_select(driver, pool, con, &res, sql, sequentialAccess)
    guard rc == APR_SUCCESS, res != nil else {
      throw AdaptorChannelError.QueryFailed(sql: sql, error: APRError(status))
    }
    
    // query was OK, collect results
    do {
      try fetchRows(res, optAttrs, cb: cb)
    }
    catch {
      throw AdaptorChannelError.QueryFailed(sql: sql, error: error)
    }
  }
  
  
  // MARK: - Escaping
  
  public func escape(string: String) -> String {
    var pool : OpaquePointer? = nil
    let status = apr_pool_create_ex(&pool, self.pool, nil, nil)
    guard status == APR_SUCCESS else { return "" } // cannot really fail?
    defer { if let p = pool { apr_pool_destroy(p) } }
    
    guard let cstr = apr_dbd_escape(driver, pool, string, con) else {
      return string // Hm.
    }
    return String(cString: cstr)
  }
  
  
  // MARK: - Errors
  
  func message(for error: Int32) -> String? {
    let cstr = apr_dbd_error(driver, con, error)
    return cstr != nil ? String(cString: cstr!) : nil
  }
  
  
  // MARK: - Evaluate Expressions
  
  func cStringsForBindVariables(_ binds : [ SQLExpression.BindVariable ],
                                pool    : OpaquePointer)
       -> UnsafeMutablePointer<UnsafePointer<Int8>?>?
  {
    // TODO: This one creates strings for the variables. We need one which
    //       generates the binary values.
    guard !binds.isEmpty else { return nil }
    // UnsafeMutablePointer<UnsafePointer<Int8>?>!
    
    guard let cStrArray =
       apr_palloc(pool, MemoryLayout<UnsafePointer<Int8>?>.stride * binds.count)
     else { return nil }
    
    let base = cStrArray
                 .assumingMemoryBound(to: Optional<UnsafePointer<Int8>>.self)
    
    var ptr = base
    for bind in binds {
      let s : String
      
      // TODO: Add a protocol to do this?
      if let value = bind.value {
        if let value = value as? String {
          s = value
        }
        else if let value = value as? SingleIntKeyGlobalID { // hacky
          s = String(value.value)
        }
        else { // TODO
          s = String(describing: value)
        }
      }
      else {
        s = ""
      }
      
      ptr.pointee = UnsafePointer(apr_pstrdup(pool, s))
      ptr = ptr.advanced(by: 1)
    }
    
    return base
  }
  
  /**
   * A primary fetch method.
   *
   * Creates a PreparedStatement from the statement and the bindings of the
   * SQLExpression.
   *
   * @param _sqlexpr - the SQLExpression to execute
   * @return the fetch results as a List of Maps
   */
  open func evaluateQueryExpression(_ sqlexpr  : SQLExpression,
                                    _ optAttrs : [ Attribute ]? = nil,
                                    result: ( AdaptorRecord ) throws -> Void)
              throws
  {
    // TODO: create callback variant
    
    if sqlexpr.bindVariables.isEmpty {
      /* expression has no binds, perform a plain SQL query */
      return try querySQL(sqlexpr.statement, optAttrs, cb: result)
    }
    
    /* setup local pool */
    
    var pool : OpaquePointer? = nil
    let status = apr_pool_create_ex(&pool, self.pool, nil, nil)
    guard status == APR_SUCCESS else { throw APRError(status) }
    defer { if let p = pool { apr_pool_destroy(p) } }
    
    /* create prepared statement */
    // TBD: cache prepared statement? Use SQL as Label? Or a hash? Probably.
    
    if logSQL { log.log("SQL: \(sqlexpr.statement)") }
    
    var stmt : OpaquePointer? = nil // UnsafePointer<apr_dbd_prepared_t>?
    let rc = apr_dbd_prepare(driver, pool, con, sqlexpr.statement,
                             nil /* label, required? */,
                             &stmt)
    guard rc == 0, stmt != nil else { throw APRError(rc) }

    /* execute */
    
    // TODO: Use binary variant
    let args = cStringsForBindVariables(sqlexpr.bindVariables, pool: pool!)
    if logSQL { log.log("SQL:  bind \(sqlexpr.bindVariables)") }
    
    var res : OpaquePointer? = nil // UnsafePointer<apr_dbd_results_t>
    let rc2 = apr_dbd_pselect(driver, pool, con, &res, stmt,
                              sequentialAccess,
                              Int32(sqlexpr.bindVariables.count), // no-op
                              args)
    guard rc2 == 0, res != nil else { throw APRError(rc) }

    // query was OK, collect results
    do {
      try fetchRows(res, optAttrs, cb: result)
    }
    catch {
      throw AdaptorChannelError.QueryFailed(sql: sqlexpr.statement,
                                            error: error)
    }
  }
  
  /**
   * Executes a SQL update expression, eg an INSERT, UPDATE or DELETE.
   * 
   * If the operation fails, the method returns -1 and sets the lastException
   * to the caught error.
   * 
   * @param _s - the formatted SQL expression
   * @return the number of affected records, or -1 if something failed
   */
  open func evaluateUpdateExpression(_ sqlexpr: SQLExpression) throws -> Int {
    if sqlexpr.bindVariables.isEmpty {
      /* expression has no binds, perform a plain SQL query */
      return try performSQL(sqlexpr.statement)
    }
    
    /* setup local pool */
    
    var pool : OpaquePointer? = nil
    let status = apr_pool_create_ex(&pool, self.pool, nil, nil)
    guard status == APR_SUCCESS else { throw APRError(status) }
    defer { if let p = pool { apr_pool_destroy(p) } }
    
    /* create prepared statement */
    // TBD: cache prepared statement? Use SQL as Label? Or a hash? Probably.
    
    var stmt : OpaquePointer? = nil // UnsafePointer<apr_dbd_prepared_t>?
    let rc = apr_dbd_prepare(driver, pool, con, sqlexpr.statement,
                             nil /* label, required? */,
                             &stmt)
    guard rc == 0, stmt != nil else { throw APRError(rc) }

    /* execute */
    
    // TODO: Use binary variant
    let args = cStringsForBindVariables(sqlexpr.bindVariables, pool: pool!)

    var updateCount : Int32 = 0
    let rc2 = apr_dbd_pquery(driver, pool, con, &updateCount, stmt,
                             Int32(sqlexpr.bindVariables.count), // no-op
                             args)
    guard rc2 == 0 else { throw APRError(rc) }
    
    return Int(updateCount)
  }
  
  
  // MARK: - Reflection
  
  public func describeTableNames()    throws -> [ String ] {
    throw AdaptorChannelError.NotImplemented(#function)
  }
  public func describeSequenceNames() throws -> [ String ] {
    throw AdaptorChannelError.NotImplemented(#function)
  }
  public func describeDatabaseNames() throws -> [ String ] {
    throw AdaptorChannelError.NotImplemented(#function)
  }
  public func describeEntityWithTableName(_ table: String) throws -> Entity? {
    throw AdaptorChannelError.NotImplemented(#function)
  }
  
  
  // MARK: - Insert w/ auto-increment support

  /**
   * This method inserts the given row into the table represented by the entity.
   * To produce the INSERT statement it uses the expressionFactory() of the
   * adaptor. The keys in the record map are converted to column names by using
   * the Entity.
   * The method returns true if exactly one row was affected by the SQL
   * statement. If the operation failed the error is thrown.
   *
   * - parameters:
   *   - row:        the record which should be inserted
   *   - entity:     the entity representing the table
   *   - refetchAll: the SQL schema may have default values assigned which are
   *                 applied if the corresponding values are not in 'row'.
   *                 Enabling 'refetchAll' makes sure all attributes of the
   *                 entity are being refetched. Requires the entity!
   * - returns:  the record, potentially refetched and updated
   */
  open func insertRow(_ row: AdaptorRow, _ entity: Entity?,
                      refetchAll: Bool)
              throws -> AdaptorRow
  {
    return try defaultInsertRow(row, entity, refetchAll: refetchAll)
  }
}

fileprivate let sequentialAccess : Int32 = 0 // constant

protocol APRAttributeValue {
  static var aprType : apr_dbd_type_e { get }
}

extension String : APRAttributeValue {
  static var aprType : apr_dbd_type_e { return APR_DBD_TYPE_STRING }
}
extension CInt : APRAttributeValue {
  static var aprType : apr_dbd_type_e { return APR_DBD_TYPE_INT }
}
extension CUnsignedInt : APRAttributeValue {
  static var aprType : apr_dbd_type_e { return APR_DBD_TYPE_UINT }
}
extension CChar : APRAttributeValue {
  static var aprType : apr_dbd_type_e { return APR_DBD_TYPE_TINY }
}
extension CUnsignedChar : APRAttributeValue {
  static var aprType : apr_dbd_type_e { return APR_DBD_TYPE_UTINY }
}
extension CShort : APRAttributeValue {
  static var aprType : apr_dbd_type_e { return APR_DBD_TYPE_SHORT }
}
extension CUnsignedShort : APRAttributeValue {
  static var aprType : apr_dbd_type_e { return APR_DBD_TYPE_USHORT }
}
extension CLong : APRAttributeValue {
  static var aprType : apr_dbd_type_e { return APR_DBD_TYPE_LONG }
}
extension CUnsignedLong : APRAttributeValue {
  static var aprType : apr_dbd_type_e { return APR_DBD_TYPE_ULONG }
}
extension CLongLong : APRAttributeValue {
  static var aprType : apr_dbd_type_e { return APR_DBD_TYPE_LONGLONG }
}
extension CUnsignedLongLong : APRAttributeValue {
  static var aprType : apr_dbd_type_e { return APR_DBD_TYPE_ULONGLONG }
}
extension CDouble : APRAttributeValue {
  static var aprType : apr_dbd_type_e { return APR_DBD_TYPE_DOUBLE }
}
extension CFloat : APRAttributeValue {
  static var aprType : apr_dbd_type_e { return APR_DBD_TYPE_FLOAT }
}

extension Attribute {
  
  var aprTypeForValueType : apr_dbd_type_e? {
    guard let vType = valueType else { return nil }
    
    if let aprVType = vType as? APRAttributeValue.Type {
      return aprVType.aprType
    }
    return nil
  }
  
  var aprTypeForExternalType : apr_dbd_type_e? {
    guard let uType = externalType?.uppercased()
     else { return nil }
    
    // TODO: consider scale and such
    if uType.hasPrefix("VARCHAR")        { return APR_DBD_TYPE_STRING     }
    if uType.hasPrefix("INT")            { return APR_DBD_TYPE_INT        }
    if uType.hasPrefix("DOUBLE")         { return APR_DBD_TYPE_DOUBLE     }
    if uType.hasPrefix("FLOAT")          { return APR_DBD_TYPE_FLOAT      }
    if uType.hasPrefix("TEXT")           { return APR_DBD_TYPE_TEXT       }
    if uType.hasPrefix("TIMESTAMP WITH") { return APR_DBD_TYPE_ZTIMESTAMP }
    if uType.hasPrefix("TIMESTAMP")      { return APR_DBD_TYPE_TIMESTAMP  }
    if uType.hasPrefix("DATETIME")       { return APR_DBD_TYPE_DATETIME   }
    if uType.hasPrefix("TIME")           { return APR_DBD_TYPE_TIME       }
    if uType.hasPrefix("DATE")           { return APR_DBD_TYPE_DATE       }
    if uType.hasPrefix("BLOB")           { return APR_DBD_TYPE_BLOB       }
    if uType.hasPrefix("CLOB")           { return APR_DBD_TYPE_CLOB       }
    /*
    APR_DBD_TYPE_TINY,       /**< \%hhd : in, out: char* */
    APR_DBD_TYPE_UTINY,      /**< \%hhu : in, out: unsigned char* */
    APR_DBD_TYPE_SHORT,      /**< \%hd  : in, out: short* */
    APR_DBD_TYPE_USHORT,     /**< \%hu  : in, out: unsigned short* */
    APR_DBD_TYPE_UINT,       /**< \%u   : in, out: unsigned int* */
    APR_DBD_TYPE_LONG,       /**< \%ld  : in, out: long* */
    APR_DBD_TYPE_ULONG,      /**< \%lu  : in, out: unsigned long* */
    APR_DBD_TYPE_LONGLONG,   /**< \%lld : in, out: apr_int64_t* */
    APR_DBD_TYPE_ULONGLONG,  /**< \%llu : in, out: apr_uint64_t* */
    APR_DBD_TYPE_NULL        /**< \%pDn : in: void*, out: void** */
     */
    return nil
  }
  
}
