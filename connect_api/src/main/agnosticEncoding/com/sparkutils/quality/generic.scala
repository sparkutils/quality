package com.sparkutils.quality

import com.sparkutils.quality.impl.CallFunctionImpls
import org.apache.spark.sql.functions.{lit, typedLit}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Column, ShimUtils}

/**
 * A collection of generic runners, allowing code to be constructed with multiple RuleSuite source types.  Unlike
 * the pre 0.2.0 runner interfaces, these all serialise any RuleSuites provided.
 */
object generic {

  /**
   * Creates a column that runs the folding RuleSuite.  This also forces registering the lambda functions used by that RuleSuite.
   *
   * FolderRunner runs all output expressions for matching rules in order of salience, the startingStruct is passed ot the first
   * matching, the result passed to the second etc.  In contrast to ruleEngineRunner OutputExpressions should be lambdas with one parameter, that of the structure
   *
   * @param ruleSuite The ruleSuite with runOnPassProcessors
   * @param startingStruct This struct is passed to the first matching rule, ideally you would use the spark dsl struct function to refer to existing columns
   * @param debugMode When debugMode is enabled the resultDataType is wrapped in Array of (salience, result) pairs to ease debugging
   * @param variablesPerFunc Defaulting to 40 allows, in combination with variableFuncGroup allows customisation of handling the 64k jvm method size limitation when performing WholeStageCodeGen
   * @param variableFuncGroup Defaulting to 20
   * @param useType In the case you must use select and can't use withColumn you may provide a type directly to stop the NPE
   * @return A Column representing the QualityRules expression built from this ruleSuite
   */
  def folder[T: RuleSuiteParam](ruleSuite: T, startingStruct: Column,
                       debugMode: Boolean = false, variablesPerFunc: Int = 40,
                       variableFuncGroup: Int = 20, useType: Option[StructType] = None,
                       extraConfig: Map[String, String] = Map.empty): Column =
    CallFunctionImpls.folder(implicitly[RuleSuiteParam[T]].column(ruleSuite),
      startingStruct, debugMode, variablesPerFunc, variableFuncGroup, useType, extraConfig)

  /**
   * Creates a column that runs the RuleSuite suitable for DQ / Validation.  This also forces registering the lambda functions used by that RuleSuite.
   *
   * @param ruleSuite The Qualty RuleSuite to evaluate
   * @param variablesPerFunc Defaulting to 40, it allows, in combination with variableFuncGroup customisation of handling the 64k jvm method size limitation when performing WholeStageCodeGen.  You _shouldn't_ need it but it's there just in case.
   * @param variableFuncGroup Defaulting to 20
   * @return A Column representing the Quality DQ expression built from this ruleSuite
   */
  def dq[T: RuleSuiteParam](ruleSuite: T, variablesPerFunc: Int = 40, variableFuncGroup: Int = 20,
                            extraConfig: Map[String, String] = Map.empty): Column =
    CallFunctionImpls.dq(implicitly[RuleSuiteParam[T]].column(ruleSuite), variablesPerFunc, variableFuncGroup,
      extraConfig)

  /**
   * Creates a column that runs the RuleSuite.  This also forces registering the lambda functions used by that RuleSuite
   * @param ruleSuite The ruleSuite with runOnPassProcessors
   * @param resultDataType The type of the results from runOnPassProcessors - must be the same for all result types,
   *                       by default most fields will be nullable and encoding must follow the fields when not specified.
   * @param debugMode When debugMode is enabled the resultDataType is wrapped in Array of (salience, result)
   *                  pairs to ease debugging
   * @param variablesPerFunc Defaulting to 40 allows, in combination with variableFuncGroup allows customisation of
   *                         handling the 64k jvm method size limitation when performing WholeStageCodeGen
   * @param variableFuncGroup Defaulting to 20
   * @return A Column representing the QualityRules expression built from this ruleSuite
   */
  def engine[T: RuleSuiteParam](ruleSuite: T, resultDataType: Option[DataType] = None,
                       debugMode: Boolean = false, variablesPerFunc: Int = 40,
                       variableFuncGroup: Int = 20, extraConfig: Map[String, String] = Map.empty): Column =
    CallFunctionImpls.engine( implicitly[RuleSuiteParam[T]].column(ruleSuite),
      resultDataType, debugMode, variablesPerFunc, variableFuncGroup, extraConfig )

  /**
   * Creates a column that runs the RuleSuite.  This also forces registering the lambda functions used by that RuleSuite
   * @param ruleSuite The ruleSuite with runOnPassProcessors
   * @param resultDataType The type of the results from runOnPassProcessors - must be the same for all result types,
   *                       by default most fields will be nullable and encoding must follow the fields when not specified.   *
   * @param debugMode When debugMode is enabled the resultDataType is wrapped in Array of (salience, result)
   *                  pairs to ease debugging
   * @return A Column representing the QualityRules expression built from this ruleSuite
   */
  def engine[T: RuleSuiteParam](ruleSuite: T, resultDataType: DataType, debugMode: Boolean): Column =
    engine(ruleSuite, Some(resultDataType), debugMode = debugMode)

  /**
   * Creates a column that runs the RuleSuite.  This also forces registering the lambda functions used by that RuleSuite
   * @param ruleSuite The ruleSuite with runOnPassProcessors
   * @param resultDataType The type of the results from runOnPassProcessors - must be the same for all result types,
   *                       by default most fields will be nullable and encoding must follow the fields when not specified.   *
   * @return A Column representing the QualityRules expression built from this ruleSuite
   */
  def engine[T: RuleSuiteParam](ruleSuite: T, resultDataType: DataType): Column =
    engine(ruleSuite, Some(resultDataType))

  /**
   * Runs the ruleSuite expressions saving results as a tuple of (ruleResult: String, resultDDL: String)
   * @param ruleSuite
   * @param name
   * @return
   */
  def typedExpression[T: RuleSuiteParam](ruleSuite: T, ddlType: String, name: String = "expressionResults",
                                         extraConfig: Map[String, String] = Map.empty): Column =
    CallFunctionImpls.typedExpression(implicitly[RuleSuiteParam[T]].column(ruleSuite),
      ddlType, name, extraConfig)

  /**
   * Runs the rule directly producing a map of results as yaml
   *
   * @param ruleSuite
   * @param name
   * @param renderOptions SnakeYml rendering options
   * @return
   */
  def expression[T: RuleSuiteParam](ruleSuite: T, name: String = "expressionResults",
                                    renderOptions: Map[String, String] = Map.empty,
                                    extraConfig: Map[String, String] = Map.empty): Column =
    CallFunctionImpls.expression(implicitly[RuleSuiteParam[T]].column(ruleSuite), name, renderOptions, extraConfig)

  /**
   * Creates a column that runs the folding RuleSuite.  This also forces registering the lambda functions used by that RuleSuite.
   *
   * FolderRunner runs all output expressions for matching rules in order of salience, the startingStruct is passed ot the first
   * matching, the result passed to the second etc.  In contrast to ruleEngineRunner OutputExpressions should be lambdas with one parameter, that of the structure
   *
   * @param ruleSuite The ruleSuite with runOnPassProcessors
   * @param resultDataType the collected result type, specify this if the derived types have nullability or ordering issues.  By default, it takes the type of the last output expression
   * @param variablesPerFunc Defaulting to 40 allows, in combination with variableFuncGroup allows customisation of handling the 64k jvm method size limitation when performing WholeStageCodeGen
   * @param variableFuncGroup Defaulting to 20
   * @param flatten when resultType is an ArrayType should the result be flattened
   * @param includeNulls should nulls returned by the output expressions be included, note when flattening nulls IN the returned arrays are not filtered
   * @param useInPlaceArray defaulting to true, this replaces the array handling approach for 'array' Output Expressions
   * @param unrollInPlaceArray defaulting to false, when true, unrolls array copying for InPlaceArray usage, it _could_ be faster in some circumstances but in most cases trust the JVM's JIT
   * @param unrollOutputArraySize defaulting to 1 and only used when unrollInPlaceArray is true.  When higher, decides how many operations should be unrolled for array copies per loop, setting to a higher number than all OutputExpression array sizes would force all array copying to be inlined.  It _could_ be faster in some circumstances but in most cases trust the JVM's JIT to unroll the loops.
   * @return A Column representing the QualityRules expression built from this ruleSuite
   */
  def collector[T: RuleSuiteParam](ruleSuite: T, resultDataType: Option[DataType] = None, variablesPerFunc: Int = 40,
                    variableFuncGroup: Int = 20, flatten: Boolean = true, includeNulls: Boolean = false,
                    useInPlaceArray: Boolean = true, unrollInPlaceArray: Boolean = false,
                    unrollOutputArraySize: Int = 1, extraConfig: Map[String, String] = Map.empty): Column = {
    CallFunctionImpls.collector(implicitly[RuleSuiteParam[T]].column(ruleSuite),
      resultDataType, variablesPerFunc, variableFuncGroup, flatten, includeNulls, useInPlaceArray,
      unrollInPlaceArray, unrollOutputArraySize, extraConfig
    )
  }
}
