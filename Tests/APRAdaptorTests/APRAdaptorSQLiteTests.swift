//
//  APRAdaptorSQLiteTests.swift
//  ZeeQL
//
//  Created by Helge Hess on 24/02/17.
//  Copyright © 2017 ZeeZide GmbH. All rights reserved.
//

import XCTest
@testable import ZeeQL
@testable import APRAdaptor

class APRAdaptorSQLiteTests: XCTestCase {
  
  let printResults = true
  
  var adaptor : APRSQLite3Adaptor = {
    var pathToTestDB : String {
      #if ZEE_BUNDLE_RESOURCES
        let bundle = Bundle(for: type(of: self) as AnyClass)
        let url    = bundle.url(forResource: "testdb", withExtension: "sqlite3")
        guard let path = url?.path else { return "testdb.sqlite3" }
        return path
      #else
        let path = ProcessInfo().environment["SRCROOT"]
                   ?? FileManager.default.currentDirectoryPath
        return "\(path)/data/testdb.sqlite3"
      #endif
    }
  
    guard let a = try? APRSQLite3Adaptor(filename: pathToTestDB)
     else { assert(false, "Could not setup adaptor") }
    return a
  }()
  
  let entity : Entity = {
    let e = ModelEntity(name: "Pet", table: "pets")
    e.attributes = [
      ModelAttribute(name: "name",  externalType: "VARCHAR(255)"),
      ModelAttribute(name: "count", externalType: "INTEGER")
    ]
    e.primaryKeyAttributeNames = [ "name" ]
    return e
  }()

  func testRawAdaptorChannelQuery() {
    guard let pc = try? adaptor.openChannel()
     else {
       XCTFail("Unexpected error, could not open channel!")
       return
     }
    let channel = pc as! APRAdaptorChannel
    defer { adaptor.releaseChannel(channel) }
    
    var resultCount = 0
    do {
      try channel.querySQL("SELECT * FROM pets") { result in
        resultCount += 1
        if printResults {
          print("  Result \(resultCount):")
          dump(row: result)
        }
      }
    }
    catch {
      XCTFail("Unexpected error: \(error)")
    }
    
    XCTAssert(resultCount >= 2, "there should be at least a cat and a dog")
  }

  func testAdaptorDataSourceFindByID() {
    let ds = AdaptorDataSource(adaptor: adaptor, entity: entity)
    do {
      let cat = try ds.findBy(id: "cat")
      XCTAssertNotNil(cat)
      XCTAssertNotNil(cat!["name"])
      XCTAssertNotNil(cat!["count"])
      
      if printResults {
        if let result = cat {
          print("  Result:")
          dump(row: result)
        }
      }
    }
    catch {
      XCTFail("Unexpected error: \(error)")
    }
  }

  func testBasicReflection() {
    guard let pc = try? adaptor.openChannel()
     else {
      XCTFail("Unexpected error, could not open channel!")
      return
     }
    defer { adaptor.releaseChannel(pc) }
    
    guard let channel = pc as? APRAdaptorChannel
     else {
      XCTFail("Unexpected error, channel not a APRAdaptorChannel!")
      return
     }
    
    do {
      let dbs = try channel.describeDatabaseNames()
      if printResults { print("Databases: \(dbs)") }
      XCTAssertFalse(dbs.isEmpty, "got no databases")
      XCTAssertEqual(dbs.count, 1)
      XCTAssertEqual(dbs[0], "main")
      
      let tables = try channel.describeTableNames()
      if printResults { print("Tables: \(tables)") }
      XCTAssertFalse(tables.isEmpty, "got no tables")
      XCTAssertTrue(tables.contains("pets"), "missing 'pets' table")      
    }
    catch {
      XCTFail("Unexpected error: \(error)")
    }
  }

  func testTableReflection() {
    guard let pc = try? adaptor.openChannel()
     else {
      XCTFail("Unexpected error, could not open channel!")
      return
     }
    defer { adaptor.releaseChannel(pc) }
    let channel = pc as! APRAdaptorChannel
    
    do {
      let petsEntity = try channel.describeEntityWithTableName("pets")
      XCTAssertNotNil(petsEntity)
      if printResults {
        print("Pets:     \(petsEntity as Optional)")
        print("  pkeys: \(petsEntity!.primaryKeyAttributeNames as Optional)")
      }
      XCTAssertEqual(petsEntity!.name,         "pets")
      XCTAssertEqual(petsEntity!.externalName, "pets")
      
      let nameAttr = petsEntity![attribute: "name"]
      XCTAssertNotNil(nameAttr)
      guard nameAttr != nil else { return }
      
      XCTAssertNotNil(nameAttr?.externalType)
      if printResults {
        print("  name: \(nameAttr as Optional) " +
              "\(nameAttr?.externalType as Optional)")
      }
      XCTAssert(nameAttr!.externalType!.hasPrefix("VARCHAR"))

      let countAttr = petsEntity![attribute: "count"]
      XCTAssertNotNil(countAttr)
      XCTAssertNotNil(countAttr?.externalType)
      if printResults {
        print("  count: \(countAttr as Optional) \(countAttr?.externalType as Optional)")
      }
      XCTAssert(countAttr!.externalType!.hasPrefix("INT"))
    }
    catch {
      XCTFail("Unexpected error: \(error)")
    }
  }
  
  func testConstraintReflection() throws {
    let model   = try adaptor.fetchModel()
    let address = model[entity: "address"]
    XCTAssertNotNil(address, "missing address table")
    
    XCTAssertEqual(address?.relationships.count, 1)
    
    let relship = address?.relationships.first
    XCTAssertNotNil(relship, "missing address table relationship")
    guard relship != nil else { return }
    
    XCTAssertFalse(relship!.isToMany, "relship is 1:n, not 1:1")
    XCTAssertEqual(relship!.destinationEntity?.name, "person")
    XCTAssertEqual(relship!.joins.count, 1, "doesn't have one join")
    guard relship!.joins.count == 1 else { return }
    
    let join = relship!.joins[0]
    XCTAssertEqual(join.sourceName,      "person_id")
    XCTAssertEqual(join.destinationName, "person_id")

    XCTAssert(join.source      === address?[attribute: "person_id"])
    XCTAssert(join.destination === model[entity: "person"]?[attribute: "person_id"])
  }
}

fileprivate func dump(row: AdaptorRecord, prefix: String = "    ") {
  for ( key, value ) in row {
    print("\(prefix)\(key): \(value as Optional) [\(type(of: value))]")
  }
}
