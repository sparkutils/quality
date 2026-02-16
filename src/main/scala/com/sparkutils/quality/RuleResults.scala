package com.sparkutils.quality

import com.sparkutils.quality.impl.{OverallResultHelper, VersionedId}
import com.sparkutils.quality.impl.util.Optional

import scala.annotation.tailrec
import scala.collection.JavaConverters._

import com.sparkutils.quality.impl.util.MapOps._

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
 * This shouldn't evaluate to a fail, allows signalling a rule has been ignored
 */
case object IgnoredRule extends RuleResult

/**
 * Returned for RuleSuiteResult.overallResult when no other trigger rule has passed and a defaultProcessor has been configured (otherwise it's Failed)
 */
case object DefaultRule extends RuleResult

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
  /**
   * Processes a RuleResult for DQ
   * @param ruleResult
   * @return
   */
  def process(ruleResult: RuleResult): OverallResult = copy(currentResult = impl.OverallResultHelper.inplace(ruleResult, currentResult, probablePass))

  /**
   * Processes a RuleResult for DefaultProcessing, unlike process, processForDefault returns Passed when receiving a Passed and all other values retain the currentResult
   * @param ruleResult
   * @return
   */
  def processForDefault(ruleResult: RuleResult): OverallResult = copy(currentResult = OverallResultHelper.inplaceForDefault(ruleResult, currentResult, probablePass))

}

/**
  * Result collection for a number of rules
  * @param overallResult
  * @param ruleResults rule id -> ruleresult
  */
case class RuleSetResult(overallResult: RuleResult, ruleResults: Map[VersionedId, RuleResult]) extends Serializable {
  def getRuleResults: java.util.Map[VersionedId, RuleResult] = ruleResults.asJava
}

/**
 * Results for all rules run against a dataframe without the overallResult.  Performance differences for filtering on top level fields
 * are significant over nested structures even under Spark 3, in the region of 30-50% depending on op.
 * @param id
 * @param ruleSetResults
 */
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
trait LazyRuleSuiteResultDetails extends Serializable {
  def ruleSuiteResultDetails: RuleSuiteResultDetails
}

/**
  * Results for all rules run against a dataframe
  * @param id - the Id of the suite, all other content is mapped
  * @param overallResult
  * @param ruleSetResults
  */
case class RuleSuiteResult(id: VersionedId, overallResult: RuleResult, ruleSetResults: Map[VersionedId, RuleSetResult]) extends Serializable {
  def details: RuleSuiteResultDetails = RuleSuiteResultDetails(id, ruleSetResults)
  def getRuleSetResults: java.util.Map[VersionedId, RuleSetResult] = ruleSetResults.asJava
}

/**
 * A lazy proxy for RuleSuiteResult
 */
trait LazyRuleSuiteResult extends Serializable {
  def ruleSuiteResult: RuleSuiteResult
}

/**
 * Represents the expression results of ExpressionRunner
 * @param result the result casted to string
 * @param resultDDL the result type in ddl
 */
case class GeneralExpressionResult(result: String, resultDDL: String) {
  // provided for compatibility
  def ruleResult: String = result
}

/**
 * Represents the results of the ExpressionRunner
 * @param id
 * @param ruleSetResults
 */
case class GeneralExpressionsResult[R](id: VersionedId, ruleSetResults: Map[VersionedId, Map[VersionedId, R]]) extends Serializable {
  def getRuleSetResults: java.util.Map[VersionedId, Map[VersionedId, R]] = ruleSetResults.asJava
}

/**
 * Represents the results of the ExpressionRunner after calling strip_result_ddl
 * @param id
 * @param ruleSetResults
 */
case class GeneralExpressionsResultNoDDL(id: VersionedId, ruleSetResults: Map[VersionedId, Map[VersionedId, String]]) extends Serializable {
  def getRuleSetResults: java.util.Map[VersionedId, Map[VersionedId, String]] = ruleSetResults.asJava
}

/**
 * Represents the rule that matched a given RuleEngine result
 * @param ruleSuiteId
 * @param ruleSetId
 * @param ruleId
 */
case class SalientRule(ruleSuiteId: VersionedId, ruleSetId: VersionedId, ruleId: VersionedId)

/**
 * Results for all rules run against a DataFrame.  Note in debug mode the type of T must be Seq[(Int, ActualType)]
 * @param ruleSuiteResults Overall results from applying the engine
 * @param salientRule if it's None there is no rule which matched for this row or it's in Debug mode which will return all results.
 * @param result The result type for this row, if no rule matched this will be None, if a rule matched but the outputexpression returned null this will also be None
 */
case class RuleEngineResult[T](ruleSuiteResults: RuleSuiteResult, salientRule: Option[SalientRule], result: Option[T]) extends Serializable {
  def getSalientRule: java.util.Optional[SalientRule] = Optional.toOptional(salientRule)

  def getResult: java.util.Optional[T] = Optional.toOptional(result)
}

/**
 * Results for all rules run against a DataFrame, the RuleSuiteResult is lazily evaluated.  Note in debug mode  the type of T must be Seq[(Int, ActualType)]
 * @param lazyRuleSuiteResults Overall results from applying the engine, evealuated lazily
 * @param salientRule if it's None there is no rule which matched for this row or it's in Debug mode which will return all results.
 * @param result The result type for this row, if no rule matched this will be None, if a rule matched but the outputexpression returned null this will also be None
 */
case class LazyRuleEngineResult[T](lazyRuleSuiteResults: LazyRuleSuiteResult, salientRule: Option[SalientRule], result: Option[T]) extends Serializable {
  def getSalientRule: java.util.Optional[SalientRule] = Optional.toOptional(salientRule)

  def getResult: java.util.Optional[T] = Optional.toOptional(result)
}

/**
 * Results for all rules run against a DataFrame.  Note in debug mode the type of T must be Seq[(Int, ActualType)]
 * @param ruleSuiteResults Overall results from applying the engine
 * @param result The result type for this row, if no rule matched this will be None, if a rule matched but the outputexpression returned null this will also be None
 */
case class RuleFolderResult[T](ruleSuiteResults: RuleSuiteResult, result: Option[T]) extends Serializable {
  def getResult: java.util.Optional[T] = Optional.toOptional(result)
}

/**
 * Results for all rules run against a DataFrame, the RuleSuiteResult is lazily evaluated.  Note in debug mode  the type of T must be Seq[(Int, ActualType)]
 * @param lazyRuleSuiteResults Overall results from applying the engine, evaluated lazily
 * @param result The result type for this row, if no rule matched this will be None, if a rule matched but the outputexpression returned null this will also be None
 */
case class LazyRuleFolderResult[T](lazyRuleSuiteResults: LazyRuleSuiteResult, result: Option[T]) extends Serializable {
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

  private[quality] def update(failed: Long = failed, passed: Long = passed, softFailed: Long = softFailed, disabled: Long = disabled,
             ignored: Long = ignored, defaulted: Long = defaulted, probabilityPassed: Long = probabilityPassed,
             probabilityFailed: Long = probabilityFailed): T

  private[quality] def combineResults(other: T): T =
    update(failed = failed + other.failed, passed = passed + other.passed, softFailed = softFailed + other.softFailed,
      disabled = disabled + other.disabled, ignored = ignored + other.ignored, defaulted = defaulted + other.defaulted,
      probabilityPassed = probabilityPassed + other.probabilityPassed,
      probabilityFailed = probabilityFailed + other.probabilityFailed)

  @deprecated(since = "0.1.4", message = "This functionality will be removed as of 0.2.0")
  def combine(other: T): T

  @tailrec
  private[quality] final def processResult(ruleResult: RuleResult, probabilityPass: Double = 0.8d): T =
    ruleResult match {
      case Failed => update(failed = failed + 1)
      case Passed => update(passed = passed + 1)
      case SoftFailed => update(softFailed = softFailed + 1)
      case DisabledRule => update(disabled = disabled + 1)
      case IgnoredRule => update(ignored = ignored + 1)
      case DefaultRule => update(defaulted = defaulted + 1)
      case Probability(percentage) if (percentage >= probabilityPass) => update(probabilityPassed = probabilityPassed + 1)
      case Probability(_) => update(probabilityFailed = probabilityFailed + 1)
      case RuleResultWithProcessor(ruleResult: RuleResult, _) =>
        processResult(ruleResult, probabilityPass)
    }

}

/**
 * Rule level results aggregated across a set of rows
 */
case class RuleStatistics(rule: VersionedId, failed: Long = 0, passed: Long = 0, softFailed: Long = 0, disabled: Long = 0, ignored: Long = 0,
                                defaulted: Long = 0, probabilityPassed: Long = 0, probabilityFailed: Long = 0) extends ResultStatistics[RuleStatistics] {

  override def update(failed: Long, passed: Long, softFailed: Long, disabled: Long, ignored: Long, defaulted: Long,
                      probabilityPassed: Long, probabilityFailed: Long): RuleStatistics =
    copy(failed = failed, passed = passed, softFailed = softFailed, disabled = disabled, ignored = ignored,
      defaulted = defaulted, probabilityPassed = probabilityPassed, probabilityFailed = probabilityFailed)

  override def combine(other: RuleStatistics): RuleStatistics = combineResults(other)
}

/**
 * Aggregated RuleStatistics for a RuleSet
 */
case class RuleSetStatistics(ruleSet: VersionedId, failed: Long = 0, passed: Long = 0, softFailed: Long = 0, disabled: Long = 0, ignored: Long = 0,
                             defaulted: Long = 0, probabilityPassed: Long = 0, probabilityFailed: Long = 0, rules: Map[VersionedId, RuleStatistics] = Map.empty) extends ResultStatistics[RuleSetStatistics] {

  override def update(failed: Long, passed: Long, softFailed: Long, disabled: Long, ignored: Long, defaulted: Long, probabilityPassed: Long, probabilityFailed: Long): RuleSetStatistics =
    copy(failed = failed, passed = passed, softFailed = softFailed, disabled = disabled, ignored = ignored,
      defaulted = defaulted, probabilityPassed = probabilityPassed, probabilityFailed = probabilityFailed)

  @deprecated(since = "0.1.4", message = "This functionality will be removed as of 0.2.0")
  def process(setResult: RuleSetResult): RuleSetStatistics =
    processResult(setResult.overallResult).copy(
      rules = setResult.ruleResults.foldLeft(rules){
        case (cur, (id, ruleResult)) =>
          cur.updatedWithF(id)(_.map{
            rs =>
              rs.processResult(ruleResult)
          }.orElse(Some(RuleStatistics(id).processResult(ruleResult))))
      })

  override def combine(other: RuleSetStatistics): RuleSetStatistics = combineResults(other).copy(
    rules = other.rules.foldLeft(rules){
      case (cur, (id, ruleResult)) =>
        cur.updatedWithF(id)(_.map{
          rs =>
            rs.combine(ruleResult)
        }.orElse(Some(ruleResult)))
    }
  )

}

/**
 * Aggregated RuleSetStatistics for a complete RuleSuite
 */
case class RuleSuiteStatistics(ruleSuite: VersionedId, failed: Long = 0, passed: Long = 0, softFailed: Long = 0, disabled: Long = 0, ignored: Long = 0,
                               defaulted: Long = 0, probabilityPassed: Long = 0, probabilityFailed: Long = 0, rowCount: Long = 0, ruleSets: Map[VersionedId, RuleSetStatistics] = Map.empty) extends ResultStatistics[RuleSuiteStatistics] {

  override def update(failed: Long, passed: Long, softFailed: Long, disabled: Long, ignored: Long, defaulted: Long, probabilityPassed: Long, probabilityFailed: Long): RuleSuiteStatistics =
    copy(failed = failed, passed = passed, softFailed = softFailed, disabled = disabled, ignored = ignored,
      defaulted = defaulted, probabilityPassed = probabilityPassed, probabilityFailed = probabilityFailed)

  @deprecated(since = "0.1.4", message = "This functionality will be removed as of 0.2.0")
  def process(ruleSuiteResult: RuleSuiteResult): RuleSuiteStatistics = {
    processResult(ruleSuiteResult.overallResult).copy(rowCount = rowCount + 1,
      ruleSets = ruleSuiteResult.ruleSetResults.foldLeft(ruleSets){
        case (cur, (id, setResult)) =>
          cur.updatedWithF(id)(_.map{
            rs =>
              rs.process(setResult)
          }.orElse(Some(RuleSetStatistics(id).process(setResult))))
      }
    )
  }

  override def combine(other: RuleSuiteStatistics): RuleSuiteStatistics = combineResults(other).copy(
    rowCount = rowCount + other.rowCount,
    ruleSets = other.ruleSets.foldLeft(ruleSets){
      case (cur, (id, setResult)) =>
        cur.updatedWithF(id)(_.map{
          rs =>
            rs.combine(setResult)
        }.orElse(Some(setResult)))
    }
  )

}

/**
 * Convenience as a dataset may contain more than one rule suite
 */
case class RuleSuiteGroupStatistics(ruleSuites: Map[VersionedId, RuleSuiteStatistics] = Map.empty, rowCount: Long = 0) extends Serializable {
  @deprecated(since = "0.1.4", message = "This functionality will be removed as of 0.2.0")
  def process(ruleSuiteResult: RuleSuiteResult): RuleSuiteGroupStatistics =
    copy(rowCount = rowCount + 1, ruleSuites = ruleSuites.updatedWithF(ruleSuiteResult.id){
      _.map( _.process(ruleSuiteResult)).orElse(Some(RuleSuiteStatistics(ruleSuiteResult.id).process(ruleSuiteResult)))
    })

  def combine(other: RuleSuiteGroupStatistics): RuleSuiteGroupStatistics =
    copy(rowCount = rowCount + other.rowCount,
      ruleSuites = other.ruleSuites.foldLeft(ruleSuites){
        case (cur, (id, rs)) =>
          cur.updatedWithF(id){
            _.map( _.combine(rs)).orElse(Some(rs))
          }
      })

}

