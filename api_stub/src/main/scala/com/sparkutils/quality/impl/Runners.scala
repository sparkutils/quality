package com.sparkutils.quality.impl

import com.sparkutils.quality.RuleSuite
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Column, DataFrame}

/**
 * Placeholders for classic implementations when run on a classic session
 */
object Runners {

  def ruleRunner(ruleSuite: RuleSuite, compileEvals: Boolean = true, resolveWith: Option[DataFrame] = None,
                 variablesPerFunc: Int = 40, variableFuncGroup: Int = 20, forceRunnerEval: Boolean = false):
    Option[Column] = None

  def ruleFolderRunner(ruleSuite: RuleSuite, startingStruct: Column, compileEvals: Boolean = false,
                       debugMode: Boolean = false, resolveWith: Option[DataFrame] = None, variablesPerFunc: Int = 40,
                       variableFuncGroup: Int = 20, forceRunnerEval: Boolean = false, useType: Option[StructType] = None,
                       forceTriggerEval: Boolean = false):
    Option[Column] = None

  def ruleEngineRunner(ruleSuite: RuleSuite, resultDataType: Option[DataType] = None, compileEvals: Boolean = true,
                       debugMode: Boolean = false, resolveWith: Option[DataFrame] = None, variablesPerFunc: Int = 40,
                       variableFuncGroup: Int = 20, forceRunnerEval: Boolean = false, forceTriggerEval: Boolean = true):
    Option[Column] = None

}
