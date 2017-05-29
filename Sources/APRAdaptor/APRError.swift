//
//  APRError.swift
//  ZeeQL
//
//  Created by Helge Hess on 23/02/17.
//  Copyright © 2017 ZeeZide GmbH. All rights reserved.
//

import CAPR

public struct APRError : Swift.Error {
  
  public let code : apr_status_t
  
  init(_ status: apr_status_t) {
    self.code = status
  }
  var message : String {
    return code.statusMessage ?? "ERROR"
  }
}

public struct APRDBDError : Swift.Error {

  public let code    : apr_status_t
  public let message : String
  
  init(_ status: apr_status_t, message: String?) {
    self.code    = status
    self.message = message ?? status.statusMessage ?? "ERROR"
  }
}

public extension apr_status_t {
  
  public var statusMessage : String? {
    let size = 1024
    let buf = UnsafeMutablePointer<Int8>.allocate(capacity: size)
    apr_strerror(self, buf, size)
    buf.deallocate(capacity: size)
    return String(cString: buf)
  }
  
}
