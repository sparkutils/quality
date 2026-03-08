package com.sparkutils.quality

/**
 * Represents a grouping of RuleSuites
 * @param ruleSuites
 */
@SerialVersionUID(1L)
case class RuleSuiteGroup(ruleSuites: Map[VersionedId, RuleSuite] = Map.empty) {

}

object RuleSuiteGroup {
  def apply(ruleSuites: Set[RuleSuite]): RuleSuiteGroup = new RuleSuiteGroup(ruleSuites.map( r => r.id -> r).toMap)

  def apply(ruleSuite: RuleSuite *): RuleSuiteGroup = apply(ruleSuite.toSet)
}

/**
 * Represents a grouping of RuleSuiteResults, from a call to group_results (Spark 4 only).  Given the results may be combined for audit trail reasons from both engines
 * and dq results no overallResult equivalent is provided.  Please use overall_dq and overall_engine functions to
 * process the ruleSuiteResults, filtering the RuleSuite Ids as needed.
 * @param ruleSuites
 */
@SerialVersionUID(1L)
case class RuleSuiteGroupResults(ruleSuiteResults: Map[VersionedId, RuleSuiteResult] = Map.empty) {

}

object RuleSuiteGroupResults {
  def apply(ruleSuites: Set[RuleSuiteResult]): RuleSuiteGroupResults = new RuleSuiteGroupResults(ruleSuites.map( r => r.id -> r).toMap)

  def apply(ruleSuite: RuleSuiteResult *): RuleSuiteGroupResults = apply(ruleSuite.toSet)
}
