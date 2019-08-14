//
//  APRPostgreSQLAdaptor.swift
//  ZeeQL
//
//  Created by Helge Hess on 23/02/17.
//  Copyright © 2017-2019 ZeeZide GmbH. All rights reserved.
//

import ZeeQL

public final class APRPostgreSQLAdaptor : APRAdaptor {
  
  let _connectString : String
  override public var connectString : String { return _connectString }
  
  public init(host: String = "127.0.0.1", port: Int = 5432,
              database: String = "postgres",
              user: String = "postgres",  password: String = "") throws
  {
    var s = ""
    if !host.isEmpty     { s += " host=\(host)"         }
    if port > 0          { s += " port=\(port)"         }
    if !database.isEmpty { s += " dbname=\(database)"   }
    if !user.isEmpty     { s += " user=\(user)"         }
    if !password.isEmpty { s += " password=\(password)" }
    self._connectString = s
    
    try super.init(driver: APRDriverNames.PostgreSQL)
    
    expressionFactory = APRPostgreSQLExpressionFactory.shared
  }

  override
  public func primaryCreateChannel(pool: OpaquePointer,
                                   connection: OpaquePointer)
              throws -> APRAdaptorChannel
  {
    return try APRPostgreSQLAdaptorChannel(adaptor: self, pool: pool,
                                           driver: driver,
                                           connection: connection)
  }

  
  // MARK: - Model
  
  override public func fetchModel() throws -> Model {
    let channel = try openChannelFromPool()
    defer { releaseChannel(channel) }
    
    return try PostgreSQLModelFetch(channel: channel).fetchModel()
  }
  override public func fetchModelTag() throws -> ModelTag {
    let channel = try openChannelFromPool()
    defer { releaseChannel(channel) }
    
    return try PostgreSQLModelFetch(channel: channel).fetchModelTag()
  }
}

public final class APRPostgreSQLAdaptorChannel: APRAdaptorChannel {
  
  // MARK: - Reflection
  // TBD: this should rather be part of the adaptor? No need to subclass just
  //      to run custom SQL
  
  override public func describeSequenceNames() throws -> [ String ] {
    return try PostgreSQLModelFetch(channel: self).describeSequenceNames()
  }
  
  override public func describeDatabaseNames() throws -> [ String ] {
    return try PostgreSQLModelFetch(channel: self).describeDatabaseNames()
  }
  override public func describeTableNames() throws -> [ String ] {
    return try PostgreSQLModelFetch(channel: self).describeTableNames()
  }

  override public func describeEntityWithTableName(_ table: String) throws
                       -> Entity?
  {
    return try PostgreSQLModelFetch(channel: self)
                 .describeEntityWithTableName(table)
  }
  
  
  // MARK: - Insert w/ auto-increment support

  override
  public func insertRow(_ row: AdaptorRow, _ entity: Entity?, refetchAll: Bool)
                throws -> AdaptorRow
  {
    let attributes : [ Attribute ]? = {
      guard let entity = entity else { return nil }
      
      if refetchAll { return entity.attributes }
      
      // TBD: refetch-all if no pkeys are assigned
      guard let pkeys = entity.primaryKeyAttributeNames, !pkeys.isEmpty
       else { return entity.attributes }
      
      return entity.attributesWithNames(pkeys)
    }()
    
    let expr = APRPostgreSQLExpression(entity: entity)
    expr.prepareInsertReturningExpressionWithRow(row, attributes: attributes)
    
    var rec : AdaptorRecord? = nil
    try evaluateQueryExpression(expr, attributes) { record in
      guard rec == nil else { // multiple matched!
        throw AdaptorError.FailedToRefetchInsertedRow(
                             entity: entity, row: row)
      }
      rec = record
    }
    guard let rrec = rec else { // no record returned?
      throw AdaptorError.FailedToRefetchInsertedRow(entity: entity, row: row)
    }
    
    return rrec.asAdaptorRow
  }
}


// MARK: - SQL Expression

public final class APRPostgreSQLExpressionFactory: SQLExpressionFactory {
  
  public static let shared = APRPostgreSQLExpressionFactory()
  
  override public func createExpression(_ entity: Entity?) -> SQLExpression {
    return APRPostgreSQLExpression(entity: entity)
  }

  
  public func insertReturningStatementForRow(_ row    : AdaptorRow,
                                             _ entity : Entity?,
                                             _ attributes : [Attribute]?)
              -> SQLExpression
  {
    // This even works for non-entity, just 'returning' keys
    let e = APRPostgreSQLExpression(entity: entity)
    e.prepareInsertReturningExpressionWithRow(row, attributes: attributes)
    return e
  }
}

public final class APRPostgreSQLExpression: APRSQLExpression {
  
  override public var sqlStringForCaseInsensitiveLike : String? {
    return "ILIKE"
  }
  
  
  // MARK: - Insert w/ returning

  public func prepareInsertReturningExpressionWithRow
                (_ row: AdaptorRow, attributes attrs: [Attribute]?)
  {
    // Note: we need the entity for the table name ...
    guard entity != nil else { return }
    
    // prepareSelectExpressionWithAttributes(attrs, lock, fs)
    
    useAliases = false
    
    /* prepare columns to select */
    
    let columns : String
    
    if let attrs = attrs {
      if !attrs.isEmpty {
        listString.removeAll()
        for attr in attrs {
          self.addSelectListAttribute(attr)
        }
        columns = listString
        listString.removeAll()
      }
      else {
        columns = "*"
      }
    }
    else {
      columns = "*"
    }
    
    /* create insert */
    
    prepareInsertExpressionWithRow(row)
    
    /* add returning */
    
    statement += " RETURNING " + columns
  }

  override public func columnTypeStringForAttribute(_ attr: Attribute) -> String
  {
    if let isAutoIncrement = attr.isAutoIncrement, isAutoIncrement {
      return "SERIAL"
    }
    
    return super.columnTypeStringForAttribute(attr)
  }
}

open class APRPostgreSQLSchemaSynchronizationFactory
             : SchemaSynchronizationFactory
{
  
  /// Supported: ALTER TABLE hello ALTER COLUMN doit TYPE INT;
  override open
  var supportsDirectColumnCoercion               : Bool { return true }
  
  /// Supported: ALTER TABLE hello DROP COLUMN doit;
  override open
  var supportsDirectColumnDeletion               : Bool { return true }
  
  /// Supported: ALTER TABLE x ADD COLUMN y TEXT;
  override open
  var supportsDirectColumnInsertion              : Bool { return true  }

  override open
  var supportsDirectForeignKeyModification       : Bool { return true }
  
  /// Supported: ALTER TABLE table ALTER COLUMN column SET [NOT] NULL;
  override open
  var supportsDirectColumnNullRuleModification   : Bool { return true }
  
  /// Supported: ALTER TABLE hello RENAME COLUMN doit TO testit;
  override open
  var supportsDirectColumnRenaming               : Bool { return true }
  
}
