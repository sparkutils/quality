package com.sparkutils.quality

import com.sparkutils.quality.impl.util.Optional
import com.sparkutils.quality.impl.util.MapOps._

import scala.annotation.tailrec
import scala.collection.JavaConverters._

@SerialVersionUID(1L)
sealed trait RuleResult extends Serializable

@SerialVersionUID(1L)
case object Failed extends RuleResult
@SerialVersionUID(1L)
case object Passed extends RuleResult

/**
 * This shouldn't evaluate to a fail, think of it as Amber / Warn
 */
@SerialVersionUID(1L)
case object SoftFailed extends RuleResult

/**
 * This shouldn't evaluate to a fail, allows signalling a rule has been disabled
 */
@SerialVersionUID(1L)
case object DisabledRule extends RuleResult

/**
 * This shouldn't evaluate to a fail, allows signalling a rule has been ignored
 */
@SerialVersionUID(1L)
case object IgnoredRule extends RuleResult

/**
 * Returned for RuleSuiteResult.overallResult when no other trigger rule has passed and a defaultProcessor has been configured (otherwise it's Failed)
 */
@SerialVersionUID(1L)
case object DefaultRule extends RuleResult

/**
 * This status indicates a rule was not yet processed, ruleEngineRunner will set this state for codegen to indicate
 * that the processing has skipped this rule
 */
@SerialVersionUID(1L)
case object UnevaluatedRule extends RuleResult

/**
  * 0-1 with 1 being absolutely likely a pass
  * @param percentage
  */
@SerialVersionUID(1L)
case class Probability(percentage: Double) extends RuleResult

/**
 * Packs a rule result with a RunOnPassProcessor processor
 */
@SerialVersionUID(1L)
case class RuleResultWithProcessor(ruleResult: RuleResult, runOnPassProcessor: RunOnPassProcessor) extends RuleResult

/**
  * Result collection for a number of rules
  * @param overallResult
  * @param ruleResults rule id -> ruleresult
  */
@SerialVersionUID(1L)
case class RuleSetResult(overallResult: RuleResult, ruleResults: Map[VersionedId, RuleResult]) extends Serializable {
  def getRuleResults: java.util.Map[VersionedId, RuleResult] = ruleResults.asJava
}

/**
 * Results for all rules run against a dataframe without the overallResult.  Performance differences for filtering on top level fields
 * are significant over nested structures even under Spark 3, in the region of 30-50% depending on op.
 * @param id
 * @param ruleSetResults
 */
@SerialVersionUID(1L)
case class RuleSuiteResultDetails(id: VersionedId, ruleSetResults: Map[VersionedId, RuleSetResult]) extends Serializable {
  def getRuleSetResults: java.util.Map[VersionedId, RuleSetResult] = ruleSetResults.asJava
}

object RuleSuiteResultDetails {
  /**
   * Creates a RuleSuiteResultDetails for this ruleSuite as if all rules Passed, it may be applicable for dqLazyDetailsFactory usage.
   * @param ruleSuite
   * @return
   */
  def ifAllPassed(ruleSuite: RuleSuite): RuleSuiteResultDetails =
    RuleSuiteResultDetails(ruleSuite.id, ruleSuite.ruleSets.map{
      ruleSet => (ruleSet.id, RuleSetResult(Passed, ruleSet.rules.map(r => (r.id, Passed)).toMap))
    }.toMap)
}

/**
 * A lazy proxy for RuleSuiteResultDetails
 */
@SerialVersionUID(1L)
trait LazyRuleSuiteResultDetails extends Serializable {
  def ruleSuiteResultDetails: RuleSuiteResultDetails
}

/**
  * Results for all rules run against a dataframe
  * @param id - the Id of the suite, all other content is mapped
  * @param overallResult
  * @param ruleSetResults
  */
@SerialVersionUID(1L)
case class RuleSuiteResult(id: VersionedId, overallResult: RuleResult, ruleSetResults: Map[VersionedId, RuleSetResult]) extends Serializable {
  def details: RuleSuiteResultDetails = RuleSuiteResultDetails(id, ruleSetResults)
  def getRuleSetResults: java.util.Map[VersionedId, RuleSetResult] = ruleSetResults.asJava
}

/**
 * Represents the expression results of ExpressionRunner
 * @param result the result casted to string
 * @param resultDDL the result type in ddl
 */
@SerialVersionUID(1L)
case class GeneralExpressionResult(result: String, resultDDL: String) {
  // provided for compatibility
  def ruleResult: String = result
}

/**
 * Represents the results of the ExpressionRunner
 * @param id
 * @param ruleSetResults
 */
@SerialVersionUID(1L)
case class GeneralExpressionsResult[R](id: VersionedId, ruleSetResults: Map[VersionedId, Map[VersionedId, R]]) extends Serializable {
  def getRuleSetResults: java.util.Map[VersionedId, Map[VersionedId, R]] = ruleSetResults.asJava
}

/**
 * Represents the results of the ExpressionRunner after calling strip_result_ddl
 * @param id
 * @param ruleSetResults
 */
@SerialVersionUID(1L)
case class GeneralExpressionsResultNoDDL(id: VersionedId, ruleSetResults: Map[VersionedId, Map[VersionedId, String]]) extends Serializable {
  def getRuleSetResults: java.util.Map[VersionedId, Map[VersionedId, String]] = ruleSetResults.asJava
}

/**
 * Represents the rule that matched a given RuleEngine result
 * @param ruleSuiteId
 * @param ruleSetId
 * @param ruleId
 */
@SerialVersionUID(1L)
case class SalientRule(ruleSuiteId: VersionedId, ruleSetId: VersionedId, ruleId: VersionedId)

/**
 * Results for all rules run against a DataFrame.  Note in debug mode the type of T must be Seq[(Int, ActualType)]
 * @param ruleSuiteResults Overall results from applying the engine
 * @param salientRule if it's None there is no rule which matched for this row or it's in Debug mode which will return all results.
 * @param result The result type for this row, if no rule matched this will be None, if a rule matched but the outputexpression returned null this will also be None
 */
@SerialVersionUID(1L)
case class RuleEngineResult[T](ruleSuiteResults: RuleSuiteResult, salientRule: Option[SalientRule], result: Option[T]) extends Serializable {
  def getSalientRule: java.util.Optional[SalientRule] = Optional.toOptional(salientRule)

  def getResult: java.util.Optional[T] = Optional.toOptional(result)
}

/**
 * Results for all rules run against a DataFrame.  Note in debug mode the type of T must be Seq[(Int, ActualType)]
 * @param ruleSuiteResults Overall results from applying the engine
 * @param result The result type for this row, if no rule matched this will be None, if a rule matched but the outputexpression returned null this will also be None
 */
@SerialVersionUID(1L)
case class RuleFolderResult[T](ruleSuiteResults: RuleSuiteResult, result: Option[T]) extends Serializable {
  def getResult: java.util.Optional[T] = Optional.toOptional(result)
}

trait ResultStatistics[T <: ResultStatistics[_]] extends Serializable {
  def failed: Long
  def passed: Long
  def softFailed: Long
  def disabled: Long
  def ignored: Long
  def defaulted: Long
  def probabilityPassed: Long
  def probabilityFailed: Long
  def unevaluated: Long
}

/**
 * Rule level results aggregated across a set of rows
 */
@SerialVersionUID(1L)
case class RuleStatistics(rule: VersionedId, failed: Long = 0, passed: Long = 0, softFailed: Long = 0,
                          disabled: Long = 0, ignored: Long = 0, defaulted: Long = 0, probabilityPassed: Long = 0,
                          probabilityFailed: Long = 0, unevaluated: Long = 0) extends ResultStatistics[RuleStatistics] {
}

/**
 * Aggregated RuleStatistics for a RuleSet
 */
@SerialVersionUID(1L)
case class RuleSetStatistics(ruleSet: VersionedId, failed: Long = 0, passed: Long = 0, softFailed: Long = 0,
                             disabled: Long = 0, ignored: Long = 0, defaulted: Long = 0, probabilityPassed: Long = 0,
                             probabilityFailed: Long = 0, unevaluated: Long = 0,
                             rules: Map[VersionedId, RuleStatistics] = Map.empty) extends ResultStatistics[RuleSetStatistics] {

}

/**
 * Aggregated RuleSetStatistics for a complete RuleSuite
 */
@SerialVersionUID(1L)
case class RuleSuiteStatistics(ruleSuite: VersionedId, failed: Long = 0, passed: Long = 0, softFailed: Long = 0,
                               disabled: Long = 0, ignored: Long = 0, defaulted: Long = 0, probabilityPassed: Long = 0,
                               probabilityFailed: Long = 0, unevaluated: Long = 0,
                               rowCount: Long = 0, ruleSets: Map[VersionedId, RuleSetStatistics] = Map.empty) extends ResultStatistics[RuleSuiteStatistics] {

}

/**
 * Convenience as a dataset may contain more than one rule suite
 */
@SerialVersionUID(1L)
case class RuleSuiteGroupStatistics(ruleSuites: Map[VersionedId, RuleSuiteStatistics] = Map.empty, rowCount: Long = 0) extends Serializable {

}

