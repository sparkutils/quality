package com.sparkutils.quality.impl.imports

import com.sparkutils.quality.RuleSuite
import com.sparkutils.quality.impl.imports.ResolveUtil.checkResolveMakesSenseOrClassic
import com.sparkutils.quality.impl.{RuleRunnerImpl, RuleSuiteHelpers}
import org.apache.spark.sql.functions.{lit, typedLit}
import org.apache.spark.sql.{Column, DataFrame, ShimUtils}

trait ClassicRuleRunnerImports {

  /**
   * Creates a column that runs the RuleSuite.  This also forces registering the lambda functions used by that RuleSuite
   *
   * NOTE if using this in a Connect session resolveWith, compileEvals and forceRunnerEval are ignored and use defaults
   *
   * @param ruleSuite The Qualty RuleSuite to evaluate
   * @param compileEvals Should the rules be compiled out to interim objects - by default false
   * @param resolveWith This experimental parameter can take the DataFrame these rules will be added to and pre-resolve and optimise the sql expressions, see the documentation for details on when to and not to use this. RuleRunner does not currently do wholestagecodegen when resolveWith is used.
   * @param variablesPerFunc Defaulting to 40, it allows, in combination with variableFuncGroup customisation of handling the 64k jvm method size limitation when performing WholeStageCodeGen.  You _shouldn't_ need it but it's there just in case.
   * @param variableFuncGroup Defaulting to 20
   * @param forceRunnerEval Defaulting to false, passing true forces a simplified partially interpreted evaluation (compileEvals must be false to get fully interpreted)
   * @return A Column representing the Quality DQ expression built from this ruleSuite
   */
  def ruleRunner(ruleSuite: RuleSuite, compileEvals: Boolean = false, resolveWith: Option[DataFrame] = None,
                 variablesPerFunc: Int = 40, variableFuncGroup: Int = 20, forceRunnerEval: Boolean = false,
                 extraConfig: Map[String, String] = Map.empty): Column =
    if (checkResolveMakesSenseOrClassic(resolveWith))
      RuleRunnerImpl.ruleRunnerImplClassic(ruleSuite, compileEvals, resolveWith,
        variablesPerFunc, variableFuncGroup, forceRunnerEval, extraConfig)
    else
      ShimUtils.callFunction("dq_rule_runner", lit(RuleSuiteHelpers.serialize(ruleSuite)),
        lit(variablesPerFunc), lit(variableFuncGroup), typedLit(extraConfig))

}
