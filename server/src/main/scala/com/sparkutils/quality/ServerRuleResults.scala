package com.sparkutils.quality

import com.sparkutils.quality.impl.util.Optional

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
case class LazyRuleEngineResult[T](lazyRuleSuiteResults: LazyRuleSuiteResult, salientRule: Option[SalientRule], result: Option[T]) extends Serializable {
  def getSalientRule: java.util.Optional[SalientRule] = Optional.toOptional(salientRule)

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
