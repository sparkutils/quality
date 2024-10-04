package com.sparkutils.quality.impl.imports

import com.sparkutils.quality.{RuleSuite, impl}
import com.sparkutils.quality.impl.{FlattenStruct, PackId, ProbabilityExpr, RuleRunnerImpl}
import org.apache.spark.sql.ShimUtils.{column, expression}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.unsafe.types.UTF8String

trait RuleRunnerImports {

  /**
   * Creates a column that runs the RuleSuite.  This also forces registering the lambda functions used by that RuleSuite
   *
   * @param ruleSuite The Qualty RuleSuite to evaluate
   * @param compileEvals Should the rules be compiled out to interim objects - by default true for eval usage, wholeStageCodeGen will evaluate in place
   * @param resolveWith This experimental parameter can take the DataFrame these rules will be added to and pre-resolve and optimise the sql expressions, see the documentation for details on when to and not to use this. RuleRunner does not currently do wholestagecodegen when resolveWith is used.
   * @param variablesPerFunc Defaulting to 40, it allows, in combination with variableFuncGroup customisation of handling the 64k jvm method size limitation when performing WholeStageCodeGen.  You _shouldn't_ need it but it's there just in case.
   * @param variableFuncGroup Defaulting to 20
   * @param forceRunnerEval Defaulting to false, passing true forces a simplified partially interpreted evaluation (compileEvals must be false to get fully interpreted)
   * @return A Column representing the Quality DQ expression built from this ruleSuite
   */
  def ruleRunner(ruleSuite: RuleSuite, compileEvals: Boolean = true, resolveWith: Option[DataFrame] = None, variablesPerFunc: Int = 40, variableFuncGroup: Int = 20, forceRunnerEval: Boolean = false): Column =
    RuleRunnerImpl.ruleRunnerImpl(ruleSuite, compileEvals, resolveWith, variablesPerFunc, variableFuncGroup, forceRunnerEval)

  /**
   * The integer value for soft failed dq rules
   */
  val SoftFailedInt = RuleResultsImports.SoftFailedInt
  /**
   * The integer value for disabled dq rules
   */
  val DisabledRuleInt = RuleResultsImports.DisabledRuleInt
  /**
   * The integer value for passed dq rules
   */
  val PassedInt = RuleResultsImports.PassedInt
  /**
   * The integer value for failed dq rules
   */
  val FailedInt = RuleResultsImports.FailedInt

}

object RuleResultsImports {

  def strLit(str: String) =
    UTF8String.fromString(str)

  val strLitA = (str: Any) =>
    UTF8String.fromString(str.asInstanceOf[String])

  val packId = PackId.packId _
  val unpackId = PackId.unpack _

  val SoftFailedInt = -1
  val DisabledRuleInt = -2
  val PassedInt = 100000
  val FailedInt = 0

  val SoftFailedExpr = Literal(SoftFailedInt, IntegerType)
  val DisabledRuleExpr = Literal(DisabledRuleInt, IntegerType)
  val PassedExpr = Literal(PassedInt, IntegerType)
  val FailedExpr = Literal(FailedInt, IntegerType)

}

trait RuleRunnerFunctionImports {
  /**
   * Returns the probability from a given rule result
   * @param result
   * @return
   */
  def probability(result: Column): Column =
    column(ProbabilityExpr(expression(result)))

  /**
   * The soft_failed value
   */
  val soft_failed = column(RuleResultsImports.SoftFailedExpr)
  /**
   * The disabled_rule value
   */
  val disabled_rule = column(RuleResultsImports.DisabledRuleExpr)
  /**
   * The passed value
   */
  val passed = column(RuleResultsImports.PassedExpr)
  /**
   * The failed value
   */
  val failed = column(RuleResultsImports.FailedExpr)

  /**
   * Flattens DQ results, unpacking the nested structure into a simple relation
   * @param result
   * @return
   */
  def flatten_results(result: Column): Column =
    column(impl.FlattenResultsExpression(expression(result), FlattenStruct.ruleSuiteDeserializer))

  /**
   * Flattens rule results, unpacking the nested structure into a simple relation
   *
   * @param result
   * @return
   */
  def flatten_rule_results(result: Column): Column =
    column(impl.FlattenRulesResultsExpression(expression(result), FlattenStruct.ruleSuiteDeserializer))

  /**
   * Flattens folder results, unpacking the nested structure into a simple relation
   *
   * @param result
   * @return
   */
  def flatten_folder_results(result: Column): Column =
    column(impl.FlattenFolderResultsExpression(expression(result), FlattenStruct.ruleSuiteDeserializer))

  /**
   * Consumes a RuleSuiteResult and returns RuleSuiteDetails
   */
  def rule_suite_result_details(result: Column): Column =
    column( impl.RuleSuiteResultDetailsExpr(expression(result)) )

}