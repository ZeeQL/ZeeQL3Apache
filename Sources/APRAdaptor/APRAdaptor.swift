//
//  APRAdaptor.swift
//  ZeeQL
//
//  Created by Helge Hess on 23/02/17.
//  Copyright © 2017 ZeeZide GmbH. All rights reserved.
//

import ZeeQL
import CAPR
import CAPRUtil

public struct APRDriverNames {
  public static let SQLite3    = "sqlite3"
  public static let PostgreSQL = "pgsql"
}


/**
 * A ZeeQL adaptor which uses the Apache Portable Runtime (Util) database
 * functions.
 *
 * You don't usually use this class directly, but one of its subclasses, e.g.
 * - APRPostgreSQLAdaptor
 * - APRSQLite3Adaptor
 *
 * The `APRAdaptor` is pretty low level and doesn't support pools. If your code
 * is running as a mod_swift module from within the *Apache* webserver,
 * use `mod_dbd` and `ApacheRequestAdaptor` instead.
 *
 * Example:
 *
 *     let adaptor = try APRSQLite3Adaptor(filename: "data/OGo.sqlite3")
 *     try db.adaptor.querySQL("SELECT COUNT(*) FROM person") {
 *       print("rec: \($0)")
 *     }
 */
open class APRAdaptor : Adaptor {
  
  final public let pool   : OpaquePointer // UnsafePointer<apr_pool_t>
  final public let driver : OpaquePointer // UnsafePointer<apr_dbd_driver_t>
  
  public var expressionFactory : SQLExpressionFactory
                               = APRSQLExpressionFactory.shared
  public var model             : Model? = nil
  
  public init(driver: String) throws {
    apr_pool_initialize()
    
    var pool : OpaquePointer? = nil
    let status = apr_pool_create_ex(&pool, nil, nil, nil)
    guard status == APR_SUCCESS && pool != nil else { throw APRError(status) }
    self.pool = pool!
    
    let initStatus = apr_dbd_init(pool)
    guard initStatus == APR_SUCCESS else { throw APRError(status) }
    
    var driverHandle : OpaquePointer? = nil
    let driverLoadResult = apr_dbd_get_driver(pool, driver, &driverHandle)
    guard driverLoadResult == APR_SUCCESS && driverHandle != nil else {
      throw APRError(status)
    }
    self.driver = driverHandle!
  }
  
  deinit {
    apr_pool_destroy(pool)
    apr_pool_terminate()
  }
  
  open var connectString : String { return "" }
  
  // MARK: - Channels

  open func openChannel() throws -> AdaptorChannel {
    // the connection is part of this pool, hence must be created in advance
    var pool : OpaquePointer? = nil
    let status = apr_pool_create_ex(&pool, self.pool, nil, nil)
    guard status == APR_SUCCESS && pool != nil else {
      throw AdaptorError.CouldNotOpenChannel(APRError(status))
    }
    
    var con : OpaquePointer? = nil
    let openStatus = apr_dbd_open(driver, pool, connectString, &con)
    guard openStatus == APR_SUCCESS && con != nil else {
      apr_pool_destroy(pool!)
      throw AdaptorError.CouldNotOpenChannel(APRError(openStatus))
    }
    
    return try primaryCreateChannel(pool: pool!, connection: con!)
  }
  open func primaryCreateChannel(pool: OpaquePointer, connection: OpaquePointer)
              throws -> APRAdaptorChannel
  {
    return try APRAdaptorChannel(adaptor: self, pool: pool,
                                 driver: driver, connection: connection)
  }

  public func releaseChannel(_ channel: AdaptorChannel) {
  }
  
  
  // MARK: - Model
  
  public func fetchModel() throws -> Model {
    // TODO: reflect using SQL92 schema queries?!
    throw AdaptorError.NotImplemented(#function)
  }
  public func fetchModelTag() throws -> ModelTag {
    // fetch model and hash in memory? makes no sense, could as well just use 
    // the newly fetched model then ...
    throw AdaptorError.NotImplemented(#function)
  }
}
