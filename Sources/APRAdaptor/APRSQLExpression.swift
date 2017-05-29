//
//  APRSQLExpression.swift
//  ZeeQL
//
//  Created by Helge Hess on 23/02/2017.
//  Copyright © 2017 ZeeZide GmbH. All rights reserved.
//

import struct Foundation.Date
import ZeeQL

open class APRSQLExpressionFactory: SQLExpressionFactory {
  
  public static let shared = APRSQLExpressionFactory()
  
  override open func createExpression(_ entity: Entity?) -> SQLExpression {
    return APRSQLExpression(entity: entity)
  }
}

open class APRSQLExpression: SQLExpression {

  override open func bindVariableDictionary(for attribute: Attribute?,
                                            value: Any?)
                     -> BindVariable
  {
    var bind = BindVariable()
    bind.attribute = attribute
    bind.value     = value

    /* Patterns:
     *    %hhd (TINY INT)
     *    %hhu (UNSIGNED TINY INT)
     *    %hd  (SHORT)
     *    %hu  (UNSIGNED SHORT)
     *    %d   (INT)
     *    %u   (UNSIGNED INT)
     *    %ld  (LONG)
     *    %lu  (UNSIGNED LONG)
     *    %lld (LONG LONG)
     *    %llu (UNSIGNED LONG LONG)
     *    %f   (FLOAT, REAL)
     *    %lf  (DOUBLE PRECISION)
     *    %s   (VARCHAR)
     *    %pDt (TEXT)
     *    %pDi (TIME,
     *    %pDd (DATE)
     *    %pDa (DATETIME)
     *    %pDs (TIMESTAMP)
     *    %pDz (TIMESTAMP WITH TIME ZONE)
     *    %pDb (BLOB)
     *    %pDc (CLOB)
     *    %pDn (NULL)
     */
    if let attribute = attribute, let bp = attribute.aprBindPattern {
      bind.placeholder = bp
    }
    else if let value = value {
      if value is Int {
        bind.placeholder = "%d" // TBD: rather %llu? and other Int types
      }
      else if value is String {
        bind.placeholder = "%s"
      }
      else if value is Date {
        bind.placeholder = "%pDs" // TBD: TIMESTAMP?
      }
      else if value is SingleIntKeyGlobalID { // TODO: HACK
        bind.placeholder = "%d" // TBD: rather %llu? and other Int types
      }
      else {
        print("TODO: unexpected bind variable type: \(value) \(type(of:value))")
      }
    }
    else {
      bind.placeholder = "%pDn" // NULL
    }
    
    /* generate and add a variable name */
    
    var name : String
    if let value = value as? QualifierVariable {
      name = value.key
    }
    else if let attribute = attribute {
      name = attribute.columnName ?? attribute.name
      name += "\(bindVariables.count)"
    }
    else {
      name = "NOATTR\(bindVariables.count)"
    }
    bind.name = name
    
    return bind
  }
}

fileprivate extension Attribute {
  
  var aprBindPattern : String? {
    guard let type = externalType else { return nil } // no type info
    
    if type.hasPrefix("INT")            { return "%d"   }
    if type.hasPrefix("VARCHAR")        { return "%s"   }
    if type.hasPrefix("TEXT")           { return "%pDt" }
    if type.hasPrefix("FLOAT")          { return "%f"   }
    if type.hasPrefix("DOUBLE")         { return "%lf"  }
    if type.hasPrefix("DATETIME")       { return "%pDa" }
    if type.hasPrefix("DATE")           { return "%pDd" }
    if type.hasPrefix("TIMESTAMP WITH") { return "%pDz" }
    if type.hasPrefix("TIMESTAMP")      { return "%pDs" }
    if type.hasPrefix("TIME")           { return "%pDi" }
    if type.hasPrefix("BLOB")           { return "%pDb" }
    if type.hasPrefix("CLOB")           { return "%pDc" }
    
    print("TODO: unexpected bind column type: \(type)")
    return nil
  }
  
}
