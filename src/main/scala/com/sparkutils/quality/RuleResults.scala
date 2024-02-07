package com.sparkutils.quality

import com.sparkutils.quality.impl.VersionedId

sealed trait RuleResult extends Serializable

case object Failed extends RuleResult
case object Passed extends RuleResult

/**
 * This shouldn't evaluate to a fail, think of it as Amber / Warn
 */
case object SoftFailed extends RuleResult

/**
 * This shouldn't evaluate to a fail, allows signalling a rule has been disabled
 */
case object DisabledRule extends RuleResult

/**
  * 0-1 with 1 being absolutely likely a pass
  * @param percentage
  */
case class Probability(percentage: Double) extends RuleResult

/**
 * Packs a rule result with a RunOnPassProcessor processor
 */
case class RuleResultWithProcessor(ruleResult: RuleResult, runOnPassProcessor: impl.RunOnPassProcessor) extends RuleResult

/**
  * Probability is evaluated at over probablePass percent, defaults to 80% 0.8.
  * Passed until any failure occurs
  */
case class OverallResult(probablePass: Double = 0.8, currentResult: RuleResult = Passed) {
  def process(ruleResult: RuleResult): OverallResult = copy(currentResult = impl.OverallResult.inplace(ruleResult, currentResult, probablePass))
}

/**
  * Result collection for a number of rules
  * @param overallResult
  * @param ruleResults rule id -> ruleresult
  */
case class RuleSetResult(overallResult: RuleResult, ruleResults: Map[VersionedId, RuleResult]) extends Serializable

/**
 * Results for all rules run against a dataframe without the overallResult.  Performance differences for filtering on top level fields
 * are significant over nested structures even under Spark 3, in the region of 30-50% depending on op.
 * @param id
 * @param ruleSetResults
 */
case class RuleSuiteResultDetails(id: VersionedId, ruleSetResults: Map[VersionedId, RuleSetResult]) extends Serializable

/**
  * Results for all rules run against a dataframe
  * @param id - the Id of the suite, all other content is mapped
  * @param overallResult
  * @param ruleSetResults
  */
case class RuleSuiteResult(id: VersionedId, overallResult: RuleResult, ruleSetResults: Map[VersionedId, RuleSetResult]) extends Serializable {
  def details: RuleSuiteResultDetails = RuleSuiteResultDetails(id, ruleSetResults)
}

/**
 * Represents the expression results of ExpressionRunner
 * @param ruleResult the result casted to string
 * @param resultDDL the result type in ddl
 */
case class GeneralExpressionResult(ruleResult: String, resultDDL: String)

/**
 * Represents the results of the ExpressionRunner
 * @param id
 * @param ruleSetResults
 */
case class GeneralExpressionsResult[R](id: VersionedId, ruleSetResults: Map[VersionedId, Map[VersionedId, R]]) extends Serializable

/**
 * Represents the results of the ExpressionRunner after calling strip_result_ddl
 * @param id
 * @param ruleSetResults
 */
case class GeneralExpressionsResultNoDDL(id: VersionedId, ruleSetResults: Map[VersionedId, Map[VersionedId, String]]) extends Serializable

/**
 * Represents the rule that matched a given RuleEngine result
 * @param ruleSuiteId
 * @param ruleSetId
 * @param ruleId
 */
case class SalientRule(ruleSuiteId: VersionedId, ruleSetId: VersionedId, ruleId: VersionedId)

/**
 * Results for all rules run against a DataFrame.  Note in debug mode you should provide Array[T] instead
 * @param ruleSuiteResults Overall results from applying the engine
 * @param salientRule if it's None there is no rule which matched for this row or it's in Debug mode which will return all results.
 * @param result The result type for this row, if no rule matched this will be None, if a rule matched but the outputexpression returned null this will also be None
 */
case class RuleEngineResult[T](ruleSuiteResults: RuleSuiteResult, salientRule: Option[SalientRule], result: Option[T]) extends Serializable {
}

/**
 * Results for all rules run against a DataFrame.  Note in debug mode you should provide Array[T] instead
 * @param ruleSuiteResults Overall results from applying the engine
 * @param result The result type for this row, if no rule matched this will be None, if a rule matched but the outputexpression returned null this will also be None
 */
case class RuleFolderResult[T](ruleSuiteResults: RuleSuiteResult, result: Option[T]) extends Serializable {
}
