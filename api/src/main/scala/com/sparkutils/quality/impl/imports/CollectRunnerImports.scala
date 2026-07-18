package com.sparkutils.quality.impl.imports

import com.sparkutils.quality.RuleSuite
import com.sparkutils.quality.impl.{CallFunctionImpls, RuleSuiteHelpers}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DataType

trait CollectRunnerImports {

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
  def collectRunner(ruleSuite: RuleSuite, resultDataType: Option[DataType] = None, variablesPerFunc: Int = 40,
                    variableFuncGroup: Int = 20, flatten: Boolean = true, includeNulls: Boolean = false,
                    useInPlaceArray: Boolean = true, unrollInPlaceArray: Boolean = false,
                    unrollOutputArraySize: Int = 1, extraConfig: Map[String, String] = Map.empty): Column =
    CallFunctionImpls.collector(lit(RuleSuiteHelpers.serialize(ruleSuite)), resultDataType, variablesPerFunc,
      variableFuncGroup, flatten, includeNulls, useInPlaceArray, unrollInPlaceArray, unrollOutputArraySize, extraConfig)

}
