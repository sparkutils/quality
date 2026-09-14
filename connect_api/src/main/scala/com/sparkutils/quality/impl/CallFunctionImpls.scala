package com.sparkutils.quality.impl

import org.apache.spark.sql.{Column, ShimUtils}
import org.apache.spark.sql.functions.{lit, typedLit}
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Implementations for the call functions
 */
protected[quality] object CallFunctionImpls {

  def folder(ruleSuite: Column, startingStruct: Column,
      debugMode: Boolean = false, variablesPerFunc: Int = 40,
      variableFuncGroup: Int = 20, useType: Option[StructType] = None,
      extraConfig: Map[String, String] = Map.empty): Column =
    ShimUtils.callFunction("rule_folder_runner", ruleSuite,
      startingStruct, lit(useType.map(_.sql).getOrElse("")), lit(debugMode), lit(variablesPerFunc),
      lit(variableFuncGroup), typedLit(extraConfig)
    )

  def dq(ruleSuite: Column, variablesPerFunc: Int = 40, variableFuncGroup: Int = 20,
         extraConfig: Map[String, String] = Map.empty): Column =
    ShimUtils.callFunction("dq_rule_runner", ruleSuite,
      lit(variablesPerFunc), lit(variableFuncGroup), typedLit(extraConfig))

  def engine(ruleSuite: Column, resultDataType: Option[DataType] = None,
      debugMode: Boolean = false, variablesPerFunc: Int = 40,
      variableFuncGroup: Int = 20, extraConfig: Map[String, String] = Map.empty): Column =
    ShimUtils.callFunction("rule_engine_runner", ruleSuite,
      lit(resultDataType.map(_.sql).getOrElse("")), lit(debugMode), lit(variablesPerFunc),
      lit(variableFuncGroup), typedLit(extraConfig)
    )

  def typedExpression(ruleSuite: Column, ddlType: String, name: String = "expressionResults",
                      extraConfig: Map[String, String] = Map.empty): Column =
    ShimUtils.callFunction("typed_expression_runner", ruleSuite,
      lit(ddlType), lit(name), typedLit(extraConfig))

  def expression(ruleSuite: Column, name: String = "expressionResults",
                 renderOptions: Map[String, String] = Map.empty,
                 extraConfig: Map[String, String] = Map.empty): Column =
    ShimUtils.callFunction("expression_runner", ruleSuite, lit(name),
      typedLit(renderOptions), typedLit(extraConfig))

  def collector(ruleSuite: Column, resultDataType: Option[DataType] = None, variablesPerFunc: Int = 40,
                                   variableFuncGroup: Int = 20, flatten: Boolean = true, includeNulls: Boolean = false,
                                   useInPlaceArray: Boolean = true, unrollInPlaceArray: Boolean = false,
                                   unrollOutputArraySize: Int = 1, extraConfig: Map[String, String] = Map.empty): Column =
    ShimUtils.callFunction("collect_runner", ruleSuite,
      lit(resultDataType.map(_.sql).getOrElse("")), lit(flatten), lit(includeNulls),
      lit(variablesPerFunc), lit(variableFuncGroup), lit(useInPlaceArray),
      lit(unrollInPlaceArray), lit(unrollOutputArraySize), typedLit(extraConfig)
    )

}
