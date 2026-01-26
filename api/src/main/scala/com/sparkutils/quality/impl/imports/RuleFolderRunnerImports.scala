package com.sparkutils.quality.impl.imports

import com.sparkutils.quality.RuleSuite
import com.sparkutils.quality.impl.{RuleSuiteHelpers, Runners}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, ShimUtils}

trait RuleFolderRunnerImports {
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
  def ruleFolderRunner(ruleSuite: RuleSuite, startingStruct: Column,
                       debugMode: Boolean = false, variablesPerFunc: Int = 40,
                       variableFuncGroup: Int = 20, useType: Option[StructType] = None): Column =
    Runners.ruleFolderRunner(ruleSuite, startingStruct, compileEvals = false, debugMode, None,
      variablesPerFunc, variableFuncGroup, useType = useType).getOrElse(
      ShimUtils.callFunction("rule_folder_runner", lit(RuleSuiteHelpers.serialize(ruleSuite)),
        startingStruct, lit(useType.map(_.sql).getOrElse("")), lit(debugMode), lit(variablesPerFunc),
        lit(variableFuncGroup)
      )
    )
}
