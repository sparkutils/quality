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