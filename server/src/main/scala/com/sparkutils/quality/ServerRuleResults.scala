package com.sparkutils.quality

import com.sparkutils.quality.RuleSuite.defaultProbablePass
import com.sparkutils.quality.impl.util.MapOps.MapOps
import com.sparkutils.quality.impl.util.Optional

import scala.annotation.tailrec

/**
 * A lazy proxy for RuleSuiteResult
 */
trait LazyRuleSuiteResult extends Serializable {
  def ruleSuiteResult: RuleSuiteResult
}

/**
 * Results for all rules run against a DataFrame, the RuleSuiteResult is lazily evaluated.  Note in debug mode  the type of T must be Seq[(Int, ActualType)]
 * @param lazyRuleSuiteResults Overall results from applying the engine, evealuated lazily
 * @param salientRule if it's None there is no rule which matched for this row or it's in Debug mode which will return all results.
 * @param result The result type for this row, if no rule matched this will be None, if a rule matched but the outputexpression returned null this will also be None
 */
@SerialVersionUID(1L)
case class LazyRuleEngineResult[T](lazyRuleSuiteResults: LazyRuleSuiteResult, salientRule: Option[SalientRule], result: Option[T]) extends Serializable {
  def getSalientRule: java.util.Optional[SalientRule] = Optional.toOptional(salientRule)

  def getResult: java.util.Optional[T] = Optional.toOptional(result)
}

/**
 * Results for all rules run against a DataFrame, the RuleSuiteResult is lazily evaluated.  Note in debug mode  the type of T must be Seq[(Int, ActualType)]
 * @param lazyRuleSuiteResults Overall results from applying the engine, evaluated lazily
 * @param result The result type for this row, if no rule matched this will be None, if a rule matched but the outputexpression returned null this will also be None
 */
@SerialVersionUID(1L)
case class LazyRuleFolderResult[T](lazyRuleSuiteResults: LazyRuleSuiteResult, result: Option[T]) extends Serializable {
  def getResult: java.util.Optional[T] = Optional.toOptional(result)
}

sealed trait ResultStatisticsProviderImpl[T <: ResultStatistics[_]] extends Serializable {

  def update(t: T)(failed: Long = t.failed, passed: Long = t.passed, softFailed: Long = t.softFailed, disabled: Long = t.disabled,
             ignored: Long = t.ignored, defaulted: Long = t.defaulted, probabilityPassed: Long = t.probabilityPassed,
             probabilityFailed: Long = t.probabilityFailed, unevaluated: Long = t.unevaluated): T

  def combineResults(t: T, other: T): T =
    update(t)(failed = t.failed + other.failed, passed = t.passed + other.passed, softFailed = t.softFailed + other.softFailed,
      disabled = t.disabled + other.disabled, ignored = t.ignored + other.ignored, defaulted = t.defaulted + other.defaulted,
      probabilityPassed = t.probabilityPassed + other.probabilityPassed,
      probabilityFailed = t.probabilityFailed + other.probabilityFailed,
      unevaluated = t.unevaluated + other.unevaluated
    )

  def combine(t: T, other: T): T

  @tailrec
  final def processResult(t: T)(ruleResult: RuleResult, probabilityPass: Double = defaultProbablePass): T =
    ruleResult match {
      case Failed => update(t)(failed = t.failed + 1)
      case Passed => update(t)(passed = t.passed + 1)
      case SoftFailed => update(t)(softFailed = t.softFailed + 1)
      case DisabledRule => update(t)(disabled = t.disabled + 1)
      case IgnoredRule => update(t)(ignored = t.ignored + 1)
      case DefaultRule => update(t)(defaulted = t.defaulted + 1)
      case Probability(percentage) if (percentage >= probabilityPass) => update(t)(probabilityPassed = t.probabilityPassed + 1)
      case Probability(_) => update(t)(probabilityFailed = t.probabilityFailed + 1)
      case RuleResultWithProcessor(ruleResult: RuleResult, _) =>
        processResult(t)(ruleResult, probabilityPass)
      case UnevaluatedRule => update(t)(unevaluated = t.unevaluated + 1)
    }

}

sealed trait ResultStatisticsProvider[T, R] extends Serializable {

  def process(t: T, result: R): T

  def combine(t: T, other: T): T

}

object ResultStatisticsProvider {
  implicit val ruleStatistics = new ResultStatisticsProviderImpl[RuleStatistics] with
    ResultStatisticsProvider[RuleStatistics, RuleResult]  {

    override def update(t: RuleStatistics)(failed: Long, passed: Long, softFailed: Long, disabled: Long, ignored: Long,
                                           defaulted: Long, probabilityPassed: Long, probabilityFailed: Long, unevaluated: Long): RuleStatistics =
      t.copy(failed = failed, passed = passed, softFailed = softFailed, disabled = disabled, ignored = ignored,
        defaulted = defaulted, probabilityPassed = probabilityPassed, probabilityFailed = probabilityFailed,
        unevaluated = unevaluated
      )

    override def combine(t: RuleStatistics, other: RuleStatistics): RuleStatistics = combineResults(t, other)

    override def process(t: RuleStatistics, result: RuleResult): RuleStatistics = processResult(t)(result)
  }

  implicit val ruleSetStatistics = new ResultStatisticsProviderImpl[RuleSetStatistics] with
    ResultStatisticsProvider[RuleSetStatistics, RuleSetResult] {

    override def update(t: RuleSetStatistics)(failed: Long, passed: Long, softFailed: Long, disabled: Long, ignored: Long,
                                              defaulted: Long, probabilityPassed: Long, probabilityFailed: Long, unevaluated: Long): RuleSetStatistics =
      t.copy(failed = failed, passed = passed, softFailed = softFailed, disabled = disabled, ignored = ignored,
        defaulted = defaulted, probabilityPassed = probabilityPassed, probabilityFailed = probabilityFailed,
        unevaluated = unevaluated)

    override def combine(t: RuleSetStatistics, other: RuleSetStatistics): RuleSetStatistics = combineResults(t, other).copy(
      rules = other.rules.foldLeft(t.rules){
        case (cur, (id, ruleResult)) =>
          cur.updatedWithF(id)(_.map{
            rs =>
              ruleStatistics.combine(rs, ruleResult)
          }.orElse(Some(ruleResult)))
      }
    )

    def process(t: RuleSetStatistics, setResult: RuleSetResult): RuleSetStatistics =
      processResult(t)(setResult.overallResult).copy(
        rules = setResult.ruleResults.foldLeft(t.rules){
          case (cur, (id, ruleResult)) =>
            cur.updatedWithF(id)(_.map{
              rs =>
                ruleStatistics.processResult(rs)(ruleResult)
            }.orElse(Some(ruleStatistics.processResult(RuleStatistics(id))(ruleResult))))
        })
  }

  implicit val ruleSuiteStatistics = new ResultStatisticsProviderImpl[RuleSuiteStatistics] with
    ResultStatisticsProvider[RuleSuiteStatistics, RuleSuiteResult] {

    override def update(t: RuleSuiteStatistics)(failed: Long, passed: Long, softFailed: Long, disabled: Long, ignored: Long,
                                                defaulted: Long, probabilityPassed: Long, probabilityFailed: Long, unevaluated: Long): RuleSuiteStatistics =
      t.copy(failed = failed, passed = passed, softFailed = softFailed, disabled = disabled, ignored = ignored,
        defaulted = defaulted, probabilityPassed = probabilityPassed, probabilityFailed = probabilityFailed,
        unevaluated = unevaluated)

    override def combine(t: RuleSuiteStatistics, other: RuleSuiteStatistics): RuleSuiteStatistics = combineResults(t, other).copy(
      rowCount = t.rowCount + other.rowCount,
      ruleSets = other.ruleSets.foldLeft(t.ruleSets){
        case (cur, (id, setResult)) =>
          cur.updatedWithF(id)(_.map{
            rs =>
              ruleSetStatistics.combine(rs, setResult)
          }.orElse(Some(setResult)))
      }
    )

    def process(t: RuleSuiteStatistics, ruleSuiteResult: RuleSuiteResult): RuleSuiteStatistics = {
      processResult(t)(ruleSuiteResult.overallResult).copy(rowCount = t.rowCount + 1,
        ruleSets = ruleSuiteResult.ruleSetResults.foldLeft(t.ruleSets){
          case (cur, (id, setResult)) =>
            cur.updatedWithF(id)(_.map{
              rs =>
                ruleSetStatistics.process(rs, setResult)
            }.orElse(Some(ruleSetStatistics.process(RuleSetStatistics(id), setResult))))
        }
      )
    }
  }

  implicit val ruleSuiteGroupStatistics = new ResultStatisticsProvider[RuleSuiteGroupStatistics, RuleSuiteResult] {

    def process(t: RuleSuiteGroupStatistics, ruleSuiteResult: RuleSuiteResult): RuleSuiteGroupStatistics =
      t.copy(rowCount = t.rowCount + 1, ruleSuites = t.ruleSuites.updatedWithF(ruleSuiteResult.id){
        _.map( t => ruleSuiteStatistics.process(t, ruleSuiteResult)).orElse(
          Some(ruleSuiteStatistics.process(RuleSuiteStatistics(ruleSuiteResult.id), ruleSuiteResult))
        )
      })

    def combine(t: RuleSuiteGroupStatistics, other: RuleSuiteGroupStatistics): RuleSuiteGroupStatistics =
      t.copy(rowCount = t.rowCount + other.rowCount,
        ruleSuites = other.ruleSuites.foldLeft(t.ruleSuites){
          case (cur, (id, rs)) =>
            cur.updatedWithF(id){
              _.map( t => ruleSuiteStatistics.combine(t, rs) ).orElse(Some(rs))
            }
        })

  }

  /**
   * wraps the providers
   * @param t
   * @param provider
   * @tparam T
   * @tparam R
   */
  implicit class ResultStatisticOps[T, R](val t: T)(implicit provider: ResultStatisticsProvider[T, R]) {
    def process(result: R): T = provider.process(t, result)

    def combine(other: T): T = provider.combine(t, other)
  }

}
