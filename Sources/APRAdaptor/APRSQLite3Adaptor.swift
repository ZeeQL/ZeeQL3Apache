//
//  APRSQLite3Adaptor.swift
//  ZeeQL
//
//  Created by Helge Hess on 23/02/2017.
//  Copyright © 2017 ZeeZide GmbH. All rights reserved.
//

import ZeeQL

open class APRSQLite3Adaptor : APRAdaptor {
  
  let filename : String
  override open var connectString : String { return filename }
  
  public init(filename: String) throws {
    self.filename = filename
    try super.init(driver: APRDriverNames.SQLite3)
  }

  override
  open func primaryCreateChannel(pool: OpaquePointer, connection: OpaquePointer)
              throws -> APRAdaptorChannel
  {
    return try APRSQLite3AdaptorChannel(adaptor: self, pool: pool,
                                        driver: driver, connection: connection)
  }

  // MARK: - Model
  
  override public func fetchModel() throws -> Model {
    let channel = try openChannelFromPool()
    defer { releaseChannel(channel) }
    
    return try SQLite3ModelFetch(channel: channel).fetchModel()
  }
  override public func fetchModelTag() throws -> ModelTag {
    let channel = try openChannelFromPool()
    defer { releaseChannel(channel) }
    
    return try SQLite3ModelFetch(channel: channel).fetchModelTag()
  }
}

public final class APRSQLite3AdaptorChannel: APRAdaptorChannel {

  // MARK: - Reflection
  // TBD: this should rather be part of the adaptor? No need to subclass just
  //      to run custom SQL
  
  override public func describeSequenceNames() throws -> [ String ] {
    return try SQLite3ModelFetch(channel: self).describeSequenceNames()
  }
  
  override public func describeDatabaseNames() throws -> [ String ] {
    return try SQLite3ModelFetch(channel: self).describeDatabaseNames(like: nil)
  }
  override public func describeTableNames() throws -> [ String ] {
    return try SQLite3ModelFetch(channel: self).describeTableNames(like: nil)
  }

  override public func describeEntityWithTableName(_ table: String) throws
                       -> Entity?
  {
    return try SQLite3ModelFetch(channel: self)
                 .describeEntityWithTableName(table)
  }
  
  
  // MARK: - Insert w/ auto-increment support

  override
  open func insertRow(_ row: AdaptorRow, _ entity: Entity?, refetchAll: Bool)
              throws -> AdaptorRow
  {
    // Mostly a copy of ZeeQL.SQLite3AdaptorChannel, but we want to keep those
    // separate.
    
    if refetchAll && entity == nil {
      throw AdaptorError.InsertRefetchRequiresEntity
    }
    
    let expr = expressionFactory.insertStatementForRow(row, entity)
    
    // In SQLite we need a transaction for the refetch
    var didOpenTx = false
    if !isTransactionInProgress {
      try begin()
      didOpenTx = true
    }
    
    let result : AdaptorRow
    do {
      guard try evaluateUpdateExpression(expr) == 1 else {
        throw AdaptorError.OperationDidNotAffectOne
      }

      if let entity = entity {
        let pkey : AdaptorRow
        if let epkey = entity.primaryKeyForRow(row), !epkey.isEmpty {
          // already had the primary key assigned
          pkey = epkey
        }
        else if let pkeys = entity.primaryKeyAttributeNames, pkeys.count == 1 {
          let expr   = expressionFactory.createExpression(entity)
          let table  = entity.externalName ?? entity.name
          let qtable = expr.sqlStringFor(schemaObjectName: table)
          
          var lastRowId : Int? = nil
          try select("SELECT last_insert_rowid() FROM \(qtable) LIMIT 1") {
            ( pkey : Int ) in
            lastRowId = pkey
          }
          guard let pkeyValue = lastRowId else {
            throw AdaptorError.FailedToGrabNewPrimaryKey(entity: entity,
                                                         row: row)
          }
          pkey = [ pkeys[0] : pkeyValue ]
        }
        else {
          throw AdaptorError.FailedToGrabNewPrimaryKey(entity: entity, row: row)
        }
        
        if refetchAll {
          let q  = qualifierToMatchAllValues(pkey)
          let fs = ModelFetchSpecification(entity: entity, qualifier: q,
                                           sortOrderings: nil, limit: 2)
          var rec : AdaptorRecord? = nil
          try selectAttributes(entity.attributes, fs, lock: false, entity) {
            record in
            guard rec == nil else { // multiple matched!
              throw AdaptorError.FailedToRefetchInsertedRow(
                                   entity: entity, row: row)
            }
            rec = record
          }
          guard let rrec = rec else { // none matched!
            throw AdaptorError.FailedToRefetchInsertedRow(
                                 entity: entity, row: row)
          }
          
          result = rrec.asAdaptorRow
        }
        else {
          result = pkey
        }
      }
      else {
        // Note: we don't know the pkey w/o entity and we don't want to reflect in
        //       here
        result = row
      }
    }
    catch {
      if didOpenTx { try? rollback() } // throw the other error
      didOpenTx = false
      throw error
    }
    
    if didOpenTx { try commit() }
    return result
  }
}
