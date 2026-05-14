package com.sparkutils.quality.impl

import com.sparkutils.quality.{RuleSuite, classicFunctions}
import com.sparkutils.testing.ConnectWhenForced.someOrForcedConnect
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Column, DataFrame}

/**
 * forwards to the server side implementations, when used in server, where stub is dropped, jar resolveWith can work
 */
object Runners {

  def ruleRunner(ruleSuite: RuleSuite, compileEvals: Boolean = false, resolveWith: Option[DataFrame] = None,
                 variablesPerFunc: Int = 40, variableFuncGroup: Int = 20, forceRunnerEval: Boolean = false,
                 extraConfig: Map[String, String] = Map.empty):
    Option[Column] = someOrForcedConnect(classicFunctions.ruleRunner(ruleSuite, compileEvals, resolveWith, variablesPerFunc,
      variableFuncGroup, forceRunnerEval, extraConfig))

  def ruleFolderRunner(ruleSuite: RuleSuite, startingStruct: Column, compileEvals: Boolean = false,
                       debugMode: Boolean = false, resolveWith: Option[DataFrame] = None, variablesPerFunc: Int = 40,
                       variableFuncGroup: Int = 20, forceRunnerEval: Boolean = false, useType: Option[StructType] = None,
                       forceTriggerEval: Boolean = false, extraConfig: Map[String, String] = Map.empty): Option[Column] =
    someOrForcedConnect(classicFunctions.ruleFolderRunner(ruleSuite, startingStruct, compileEvals, debugMode, resolveWith, variablesPerFunc,
      variableFuncGroup, forceRunnerEval, useType, forceTriggerEval, extraConfig))

  def ruleEngineRunner(ruleSuite: RuleSuite, resultDataType: Option[DataType] = None, compileEvals: Boolean = false,
                       debugMode: Boolean = false, resolveWith: Option[DataFrame] = None, variablesPerFunc: Int = 40,
                       variableFuncGroup: Int = 20, forceRunnerEval: Boolean = false, forceTriggerEval: Boolean = false,
                       extraConfig: Map[String, String] = Map.empty):
    Option[Column] = someOrForcedConnect(classicFunctions.ruleEngineRunner(ruleSuite, resultDataType, compileEvals, debugMode,
      resolveWith, variablesPerFunc, variableFuncGroup, forceRunnerEval, forceTriggerEval, extraConfig))

}
