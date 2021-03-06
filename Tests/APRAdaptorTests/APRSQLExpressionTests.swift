//
//  APRAdaptorTests.swift
//  APRAdaptorTests
//
//  Created by Helge Hess on 24/02/2017.
//  Copyright © 2017 ZeeZide GmbH. All rights reserved.
//

import XCTest
@testable import ZeeQL
@testable import APRAdaptor

class APRSQLExpressionTests: XCTestCase {
    
  let factory = APRSQLExpressionFactory()
  
  let entity  : Entity = {
    let e = ModelEntity(name: "company")
    e.attributes = [
      ModelAttribute(name: "id",   externalType: "INTEGER"),
      ModelAttribute(name: "age",  externalType: "INTEGER"),
      ModelAttribute(name: "name", externalType: "VARCHAR(255)")
    ]
    return e
  }()
  
  
  func testRawDeleteSQLExpr() {
    let q = qualifierWith(format: "id = 5")
    XCTAssertNotNil(q, "could not parse qualifier")
    
    let expr = factory.deleteStatementWithQualifier(q!, entity)
    XCTAssertEqual(expr.statement,
                   "DELETE FROM \"company\" WHERE \"id\" = 5",
                   "unexpected SQL result")
  }
  
  func testUpdateSQLExpr() {
    let q = qualifierWith(format: "id = 5")
    XCTAssertNotNil(q, "could not parse qualifier")
    
    let row : [ String : Any? ] = [ "age": 42, "name": "Zealandia" ]
    
    let expr = factory.updateStatementForRow(row, q!, entity)
    
    XCTAssertEqual(expr.statement,
      "UPDATE \"company\" SET \"age\" = 42, \"name\" = %s WHERE \"id\" = 5",
      "unexpected SQL result")
    
    let bindings = expr.bindVariables
    XCTAssertEqual(bindings.count, 1, "unexpected binding count")
    XCTAssertEqual(bindings[0].value as? String, "Zealandia")
  }

  func testInsertSQLExpr() {
    let row : [ String : Any? ] = [ "id": 5, "age": 42, "name": "Zealandia" ]
    
    let expr = factory.insertStatementForRow(row, entity)
    XCTAssertEqual(expr.statement,
                   "INSERT INTO \"company\" ( \"id\", \"age\", \"name\" ) " +
                   "VALUES ( 5, 42, %s )",
                   "unexpected SQL result")
    
    let bindings = expr.bindVariables
    XCTAssertEqual(bindings.count, 1, "unexpected binding count")
    XCTAssertEqual(bindings[0].value as? String, "Zealandia")
  }
  
  func testSimpleSelectExpr() {
    let q = qualifierWith(format: "age > 13")
    XCTAssertNotNil(q, "could not parse qualifier")
    
    let fs = ModelFetchSpecification(entity: entity, qualifier: q)
    let expr = factory.selectExpressionForAttributes(
      entity.attributes, lock: true, fs, entity
    )
    XCTAssertEqual(expr.statement,
      "SELECT BASE.\"id\", BASE.\"age\", BASE.\"name\" " +
        "FROM \"company\" AS BASE " +
       "WHERE BASE.\"age\" > 13 FOR UPDATE",
      "unexpected SQL result")
  }
  
  func testSimpleSelectExprWithArgument() {
    let q = qualifierWith(format: "name = %@", "Donald")
    XCTAssertNotNil(q, "could not parse qualifier")
    
    let fs = ModelFetchSpecification(entity: entity, qualifier: q)
    let expr = factory.selectExpressionForAttributes(
      entity.attributes, lock: true, fs, entity
    )
    XCTAssertEqual(expr.statement,
                   "SELECT BASE.\"id\", BASE.\"age\", BASE.\"name\" " +
                     "FROM \"company\" AS BASE " +
                    "WHERE BASE.\"name\" = %s FOR UPDATE",
                   "unexpected SQL result")
    let bindings = expr.bindVariables
    XCTAssertEqual(bindings.count, 1, "unexpected binding count")
    XCTAssertEqual(bindings[0].value as? String, "Donald")
  }
}
