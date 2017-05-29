//
//  APRAdaptorOGoTests.swift
//  ZeeQL
//
//  Created by Helge Hess on 24/02/17.
//  Copyright © 2017 ZeeZide GmbH. All rights reserved.
//

import XCTest
@testable import ZeeQL
@testable import APRAdaptor

class APRAdaptorOGoTests: AdaptorOGoTestCase {
  
  override var adaptor : Adaptor! {
    XCTAssertNotNil(_adaptor)
    return _adaptor
  }

  let _adaptor = try! APRPostgreSQLAdaptor(database: "OGo2",
                                           user: "OGo", password: "OGo")
}

