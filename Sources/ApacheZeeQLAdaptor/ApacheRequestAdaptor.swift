//
//  ApacheRequestAdaptor.swift
//  ZeeQL
//
//  Created by Helge Hess on 03.04.17.
//  Copyright © 2017 ZeeZide GmbH. All rights reserved.
//

import APRAdaptor
import CApache
import CAPRUtil
import ZeeQL

// our funcs need to declare the type, there is no automagic macro way ...
fileprivate func APR_RETRIEVE_OPTIONAL_FN<T>(_ name: String) -> T? {
  guard let fn = apr_dynamic_fn_retrieve(name) else { return nil }
  return unsafeBitCast(fn, to: T.self) // TBD: is there a better way?
}

/**
 * A ZeeQL adaptor which hooks up ZeeQL to Apache `mod_dbd`.
 * 
 * Using `mod_dbd` has several advantages over using `APRAdaptor` directly:
 *
 * - endpoint can be configured in the regular Apache configuration
 * - does configurable pooling and connection-reuse
 * - can share connections between different Apache modules (not just Swift
 *   ones!). 
 *   E.g. if you implement a part of your app in PHP, it can still use the same
 *   pool.
 *
 * Example code (using ApacheExpress):
 *
 *     app.use(mod_dbd())
 *
 *     try req.dbAdaptor?.select("SELECT COUNT(*) FROM person") { (count:Int) in
 *       console.log("Number of persons:", count)
 *     }
 *
 * THREADING: This adaptor is scoped to a single Apache request and hence is
 *            single threaded.
 */
open class ApacheRequestAdaptor : ZeeQL.Adaptor {
  
  public enum Error : Swift.Error {
    case MissingAcquireModDBDNotAvailable
    case CouldNotAquire
    // those should never happen:
    case RequestHasNoPool
    case ConnectionHasNoHandle
    case ConnectionHasNoDriver
  }
  
  typealias aprz_OFN_ap_dbd_acquire_t = @convention(c)
    ( UnsafeMutableRawPointer? ) -> UnsafeMutablePointer<ap_dbd_t>?

  static let ap_dbd_acquire : aprz_OFN_ap_dbd_acquire_t? = {
    return APR_RETRIEVE_OPTIONAL_FN("ap_dbd_acquire")
  }()

  public let handle : OpaquePointer // UnsafeMutablePointer<request_rec>
  
  var _expressionFactory : SQLExpressionFactory?
  public var expressionFactory : SQLExpressionFactory {
    return _expressionFactory ?? APRSQLExpressionFactory.shared
  }
  public var model             : Model?

  public init(handle: UnsafeMutablePointer<request_rec>,
              model: Model? = nil) throws
  {
    guard ApacheRequestAdaptor.ap_dbd_acquire != nil else {
      throw Error.MissingAcquireModDBDNotAvailable
    }
    
    self.handle = OpaquePointer(handle)
    self.model  = model
  }
  
  public var typedHandle : UnsafeMutablePointer<request_rec>? {
    // yes, this is awkward, but we cannot store request_rec or ZzApache in an
    // instance variable, crashes swiftc
    return UnsafeMutablePointer<request_rec>(handle)
  }
  
  open func openChannel() throws -> AdaptorChannel {
    return try primaryOpenChannel()
  }
  
  open func primaryOpenChannel() throws -> AdaptorChannel {
    guard let acquire = ApacheRequestAdaptor.ap_dbd_acquire else {
      throw AdaptorError.CouldNotOpenChannel(
                          Error.MissingAcquireModDBDNotAvailable)
    }
    guard let apCon = acquire(typedHandle) else {
      throw AdaptorError.CouldNotOpenChannel(Error.CouldNotAquire)
    }
    guard let pool = typedHandle!.pointee.pool else {
      throw AdaptorError.CouldNotOpenChannel(Error.RequestHasNoPool)
    }
    guard let con = apCon.pointee.handle else {
      throw AdaptorError.CouldNotOpenChannel(Error.ConnectionHasNoHandle)
    }
    guard let driver = apCon.pointee.driver else {
      throw AdaptorError.CouldNotOpenChannel(Error.ConnectionHasNoDriver)
    }
    
    if let driverName = apr_dbd_name(driver) {
      if strcmp(driverName, APRDriverNames.SQLite3) == 0 {
        if _expressionFactory == nil {
          _expressionFactory = APRSQLExpressionFactory.shared
        }
        return try APRSQLite3AdaptorChannel(adaptor: self, pool: pool,
                                            driver: driver, connection: con,
                                            ownsPool: false,
                                            ownsConnection: false)
      }
      if strcmp(driverName, APRDriverNames.PostgreSQL) == 0 {
        if _expressionFactory == nil {
          _expressionFactory = APRPostgreSQLExpressionFactory.shared
        }
        return try APRPostgreSQLAdaptorChannel(adaptor: self, pool: pool,
                                               driver: driver, connection: con,
                                               ownsPool: false,
                                               ownsConnection: false)
      }
    }
    
    if _expressionFactory == nil {
      _expressionFactory = APRSQLExpressionFactory.shared
    }
    return try APRAdaptorChannel(adaptor: self, pool: pool,
                                 driver: driver, connection: con,
                                 ownsPool: false, ownsConnection: false)
  }
  
  
  // MARK: - Model
  
  public func fetchModel() throws -> Model {
    let channel = try openChannelFromPool()
    defer { releaseChannel(channel) }
    
    // TODO: maybe make the model-fetch public API to avoid all those dupes
    if let c = channel as? APRSQLite3AdaptorChannel {
      return try SQLite3ModelFetch(channel: c).fetchModel()
    }
    else if let c = channel as? APRPostgreSQLAdaptorChannel {
      return try PostgreSQLModelFetch(channel: c).fetchModel()
    }
    else {
      throw AdaptorError.NotImplemented(#function)
    }
  }
  public func fetchModelTag() throws -> ModelTag {
    let channel = try openChannelFromPool()
    defer { releaseChannel(channel) }
    
    // TODO: maybe make the model-fetch public API to avoid all those dupes
    if let c = channel as? APRSQLite3AdaptorChannel {
      return try SQLite3ModelFetch(channel: c).fetchModelTag()
    }
    else if let c = channel as? APRPostgreSQLAdaptorChannel {
      return try PostgreSQLModelFetch(channel: c).fetchModelTag()
    }
    else {
      throw AdaptorError.NotImplemented(#function)
    }
  }
  
  
  // MARK: - Request local connection pool for mod_dbd

  let maxPoolSize       = 4
  var pooledConnections = [ AdaptorChannel ]()
    // This is easy in this case, because we are single threaded and the pool
    // won't live for long.
    // We don't want to acquire connections from mod_dbd unnecessarily.
  
  public func openChannelFromPool() throws -> AdaptorChannel {
    guard !pooledConnections.isEmpty else { return try primaryOpenChannel() }
    
    return pooledConnections.removeFirst() // expensive but better
  }
  
  open func releaseChannel(_ channel: AdaptorChannel) {
    guard pooledConnections.count < maxPoolSize else { return } // do not pool
    
    // This is fine from a retain-cycle perspective, the APRAdaptorChannel
    // doesn't actually retain the adaptor (just refers to the handles).
    pooledConnections.append(channel)
  }
}
