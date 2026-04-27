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
      variableFuncGroup: Int = 20, useType: Option[StructType] = None): Column =
    ShimUtils.callFunction("rule_folder_runner", ruleSuite,
      startingStruct, lit(useType.map(_.sql).getOrElse("")), lit(debugMode), lit(variablesPerFunc),
      lit(variableFuncGroup)
    )

  def dq(ruleSuite: Column, variablesPerFunc: Int = 40, variableFuncGroup: Int = 20): Column =
    ShimUtils.callFunction("dq_rule_runner", ruleSuite,
      lit(variablesPerFunc), lit(variableFuncGroup))

  def engine(ruleSuite: Column, resultDataType: Option[DataType] = None,
      debugMode: Boolean = false, variablesPerFunc: Int = 40,
      variableFuncGroup: Int = 20): Column =
    ShimUtils.callFunction("rule_engine_runner", ruleSuite,
      lit(resultDataType.map(_.sql).getOrElse("")), lit(debugMode), lit(variablesPerFunc),
      lit(variableFuncGroup)
    )

  def typedExpression(ruleSuite: Column, ddlType: String, name: String = "expressionResults"): Column =
    ShimUtils.callFunction("typed_expression_runner", ruleSuite,
      lit(ddlType), lit(name))

  def expression(ruleSuite: Column, name: String = "expressionResults",
                                    renderOptions: Map[String, String] = Map.empty): Column =
    ShimUtils.callFunction("expression_runner", ruleSuite, lit(name),
      typedLit(renderOptions))

  def collector(ruleSuite: Column, resultDataType: Option[DataType] = None, variablesPerFunc: Int = 40,
                                   variableFuncGroup: Int = 20, flatten: Boolean = true, includeNulls: Boolean = false,
                                   useInPlaceArray: Boolean = true, unrollInPlaceArray: Boolean = false,
                                   unrollOutputArraySize: Int = 1): Column =
    ShimUtils.callFunction("collect_runner", ruleSuite,
      lit(resultDataType.map(_.sql).getOrElse("")), lit(flatten), lit(includeNulls),
      lit(variablesPerFunc), lit(variableFuncGroup), lit(useInPlaceArray),
      lit(unrollInPlaceArray), lit(unrollOutputArraySize)
    )

}
