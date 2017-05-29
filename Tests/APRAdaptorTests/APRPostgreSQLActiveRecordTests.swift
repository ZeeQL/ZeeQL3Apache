//
//  APRPostgreSQLActiveRecordTests.swift
//  ZeeQL3Apache
//
//  Created by Helge Hess on 18/05/17.
//  Copyright © 2017 ZeeZide GmbH. All rights reserved.
//

import Foundation
import XCTest
@testable import ZeeQL
@testable import APRAdaptor

class APRPostgreSQLActiveRecordTests: AdapterActiveRecordTests {
  
  override var adaptor : Adaptor! { return _adaptor }
  var _adaptor : Adaptor = {
    guard let a = try? APRPostgreSQLAdaptor(database: "contacts",
                                            user: "OGo", password: "OGo")
     else { assert(false, "Could not setup adaptor") }
    return a
  }()
  
}
