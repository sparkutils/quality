package com.sparkutils.qualityTests.classicOnly

import com.sparkutils.quality.ResultStatisticsProvider.ResultStatisticOps
import com.sparkutils.quality.impl.aggregates.StatsRowOps
import com.sparkutils.quality.{DefaultRule, DisabledRule, Failed, Id, IgnoredRule, Passed, Probability, RuleSetResult, RuleSetStatistics, RuleStatistics, RuleSuiteGroupStatistics, RuleSuiteResult, RuleSuiteStatistics, SoftFailed}
import com.sparkutils.qualityTests.util.ClassicSharedTests
import org.apache.spark.sql.ShimUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.scalatest.Matchers
import com.sparkutils.quality.impl.util.MapOps._

class StatisticsTest extends ClassicSharedTests with Matchers {

  val set1 = RuleSetStatistics(Id(1,0), failed = 1, passed = 1, ignored = 1, defaulted = 1, softFailed = 1,
    disabled = 1, probabilityFailed = 1, probabilityPassed = 1, rules = Map(
      Id(1,0) -> RuleStatistics(Id(1,0), failed = 1),
      Id(2,0) -> RuleStatistics(Id(2,0), passed = 1),
      Id(3,0) -> RuleStatistics(Id(3,0), ignored = 1),
      Id(4,0) -> RuleStatistics(Id(4,0), defaulted = 1),
      Id(5,0) -> RuleStatistics(Id(5,0), softFailed = 1),
      Id(6,0) -> RuleStatistics(Id(6,0), disabled = 1),
      Id(7,0) -> RuleStatistics(Id(7,0), probabilityFailed = 1),
      Id(8,0) -> RuleStatistics(Id(8,0), probabilityPassed = 1)
    ))

  val set2 = RuleSetStatistics(Id(2,0), failed = 5, passed = 6, ignored = 7, defaulted = 8, softFailed = 9,
    disabled = 10, probabilityFailed = 11, probabilityPassed = 12, rules = Map(
      Id(9,0) -> RuleStatistics(Id(1,0), failed = 1),
      Id(10,0) -> RuleStatistics(Id(2,0), passed = 1),
      Id(11,0) -> RuleStatistics(Id(3,0), ignored = 1)
    ))

  test("setLevelOperations") {
    // should have 3 for everything
    val combined = set1.combine(set1.combine(set1))
    combined shouldBe RuleSetStatistics(Id(1,0), failed = 3, passed = 3, ignored = 3, defaulted = 3, softFailed = 3,
      disabled = 3, probabilityFailed = 3, probabilityPassed = 3, rules = Map(
        Id(1,0) -> RuleStatistics(Id(1,0), failed = 3),
        Id(2,0) -> RuleStatistics(Id(2,0), passed = 3),
        Id(3,0) -> RuleStatistics(Id(3,0), ignored = 3),
        Id(4,0) -> RuleStatistics(Id(4,0), defaulted = 3),
        Id(5,0) -> RuleStatistics(Id(5,0), softFailed = 3),
        Id(6,0) -> RuleStatistics(Id(6,0), disabled = 3),
        Id(7,0) -> RuleStatistics(Id(7,0), probabilityFailed = 3),
        Id(8,0) -> RuleStatistics(Id(8,0), probabilityPassed = 3)
      ))

    // push another result through, force a bump on disabled, bump all the others
    val updated =
      combined.process(RuleSetResult(DisabledRule, Map(
        Id(1,0) -> Failed,
        Id(2,0) -> Passed,
        Id(3,0) -> IgnoredRule,
        Id(4,0) -> DefaultRule,
        Id(5,0) -> SoftFailed,
        Id(6,0) -> DisabledRule,
        Id(7,0) -> Probability(0.4),
        Id(8,0) -> Probability(0.9)
      )))

    updated shouldBe RuleSetStatistics(Id(1,0), failed = 3, passed = 3, ignored = 3, defaulted = 3, softFailed = 3,
      disabled = 4, probabilityFailed = 3, probabilityPassed = 3, rules = Map(
        Id(1,0) -> RuleStatistics(Id(1,0), failed = 4),
        Id(2,0) -> RuleStatistics(Id(2,0), passed = 4),
        Id(3,0) -> RuleStatistics(Id(3,0), ignored = 4),
        Id(4,0) -> RuleStatistics(Id(4,0), defaulted = 4),
        Id(5,0) -> RuleStatistics(Id(5,0), softFailed = 4),
        Id(6,0) -> RuleStatistics(Id(6,0), disabled = 4),
        Id(7,0) -> RuleStatistics(Id(7,0), probabilityFailed = 4),
        Id(8,0) -> RuleStatistics(Id(8,0), probabilityPassed = 4)
      ))

    // choose a rando to update...
    val onlyOneEntry =
      updated.process(RuleSetResult(IgnoredRule, Map(
        Id(8,0) -> Passed
      )))

    onlyOneEntry shouldBe RuleSetStatistics(Id(1,0), failed = 3, passed = 3, ignored = 4, defaulted = 3, softFailed = 3,
      disabled = 4, probabilityFailed = 3, probabilityPassed = 3, rules = updated.rules.updatedWithF(Id(8,0)) {
        _.map(_.copy(passed = 1))
      })

    // combine incomplete sets, shouldn't exist but implemented to allow for incomplete results
    val rse = set1.copy( rules = set1.rules - Id(6,0), disabled = 0 )

    rse.combine( set1 ) shouldBe RuleSetStatistics(Id(1,0), failed = 2, passed = 2, ignored = 2, defaulted = 2, softFailed = 2,
      disabled = 1, probabilityFailed = 2, probabilityPassed = 2, rules = Map(
        Id(1,0) -> RuleStatistics(Id(1,0), failed = 2),
        Id(2,0) -> RuleStatistics(Id(2,0), passed = 2),
        Id(3,0) -> RuleStatistics(Id(3,0), ignored = 2),
        Id(4,0) -> RuleStatistics(Id(4,0), defaulted = 2),
        Id(5,0) -> RuleStatistics(Id(5,0), softFailed = 2),
        Id(6,0) -> RuleStatistics(Id(6,0), disabled = 1),
        Id(7,0) -> RuleStatistics(Id(7,0), probabilityFailed = 2),
        Id(8,0) -> RuleStatistics(Id(8,0), probabilityPassed = 2)
      ))
  }

  test("ruleSuiteOperations") {
    val rs1 = RuleSuiteStatistics(Id(1,0), ruleSets = Map(set1.ruleSet -> set1, set2.ruleSet -> set2))
    rs1.rowCount shouldBe 0

    val rs2 = rs1.copy(rowCount = 20).process(
      RuleSuiteResult(
        Id(1,0), Passed, Map(
          Id(1,0) ->
            RuleSetResult(DisabledRule, Map(
              Id(1,0) -> Failed,
              Id(2,0) -> Passed,
              Id(3,0) -> IgnoredRule,
              Id(4,0) -> DefaultRule,
              Id(5,0) -> SoftFailed,
              Id(6,0) -> DisabledRule,
              Id(7,0) -> Probability(0.4),
              Id(8,0) -> Probability(0.9)
            ))
        ))
    )
    val rs3 = rs1.copy(rowCount = 50)

    val combined = rs2.combine(rs3)
    combined shouldBe RuleSuiteStatistics(Id(1,0), rowCount = 71, passed = 1,
      ruleSets = Map(set1.ruleSet ->
        RuleSetStatistics(Id(1,0), failed = 2, passed = 2, ignored = 2, defaulted = 2, softFailed = 2,
          disabled = 3, probabilityFailed = 2, probabilityPassed = 2, rules = Map(
            Id(1,0) -> RuleStatistics(Id(1,0), failed = 3),
            Id(2,0) -> RuleStatistics(Id(2,0), passed = 3),
            Id(3,0) -> RuleStatistics(Id(3,0), ignored = 3),
            Id(4,0) -> RuleStatistics(Id(4,0), defaulted = 3),
            Id(5,0) -> RuleStatistics(Id(5,0), softFailed = 3),
            Id(6,0) -> RuleStatistics(Id(6,0), disabled = 3),
            Id(7,0) -> RuleStatistics(Id(7,0), probabilityFailed = 3),
            Id(8,0) -> RuleStatistics(Id(8,0), probabilityPassed = 3)
          ))
        , set2.ruleSet ->

          RuleSetStatistics(Id(2,0), failed = 10, passed = 12, ignored = 14, defaulted = 16, softFailed = 18,
            disabled = 20, probabilityFailed = 22, probabilityPassed = 24, rules = Map(
              Id(9,0) -> RuleStatistics(Id(1,0), failed = 2),
              Id(10,0) -> RuleStatistics(Id(2,0), passed = 2),
              Id(11,0) -> RuleStatistics(Id(3,0), ignored = 2)
            )))
    )

    // combine sets that don't exist, shouldn't be a real case model wise but would allow for incomplete results
    val rse = RuleSuiteStatistics(Id(1,0), ruleSets = Map(set1.ruleSet -> set1))

    (rse.combine( RuleSuiteStatistics(Id(1,0), ruleSets = Map(set2.ruleSet -> set2)) )) shouldBe rs1
  }

  test("ruleSuiteGroupOperations") {
    val rsr = RuleSuiteResult(
      Id(100,0), Passed, Map(
        Id(1,0) ->
          RuleSetResult(DisabledRule, Map(
            Id(1,0) -> Failed,
            Id(2,0) -> Passed,
            Id(3,0) -> IgnoredRule,
            Id(4,0) -> DefaultRule,
            Id(5,0) -> SoftFailed,
            Id(6,0) -> DisabledRule,
            Id(7,0) -> Probability(0.4),
            Id(8,0) -> Probability(0.9)
          ))
      ))

    val rs1 = RuleSuiteStatistics(Id(100,0), rowCount = 10, ruleSets = Map(set1.ruleSet -> set1, set2.ruleSet -> set2))
    val rs1_1 = rs1.process(rsr)

    val rs2 = RuleSuiteStatistics(Id(200,0), rowCount = 30, ruleSets = Map(set2.ruleSet -> set2))

    val grp = RuleSuiteGroupStatistics(rowCount = 10, ruleSuites = Map(rs1.ruleSuite -> rs1))

    val grp_combined = grp.combine(
      RuleSuiteGroupStatistics(rowCount = 30, ruleSuites = Map(rs2.ruleSuite -> rs2))
    )

    grp_combined shouldBe RuleSuiteGroupStatistics(Map(rs1.ruleSuite -> rs1, rs2.ruleSuite -> rs2), rowCount = 40)

    val processed = grp_combined.process(rsr)

    processed shouldBe RuleSuiteGroupStatistics(Map(rs1.ruleSuite -> rs1_1, rs2.ruleSuite -> rs2), rowCount = 41)
  }

  lazy val rgStatsSer = ShimUtils.expressionEncoder(com.sparkutils.quality.impl.Encoders.ruleSuiteGroupStatisticsTypedExpEnc).resolveAndBind().objSerializer
  lazy val rgStatsDer = ShimUtils.expressionEncoder(com.sparkutils.quality.impl.Encoders.ruleSuiteGroupStatisticsTypedExpEnc).resolveAndBind().objDeserializer
  lazy val rsResultDer = ShimUtils.expressionEncoder(com.sparkutils.quality.impl.Encoders.ruleSuiteResultExpEnc).resolveAndBind().objDeserializer
  lazy val rsResultSer = ShimUtils.expressionEncoder(com.sparkutils.quality.impl.Encoders.ruleSuiteResultExpEnc).resolveAndBind().objSerializer

  def row(rsr: RuleSuiteResult): InternalRow = {
    rsResultSer.eval(InternalRow(rsr)).asInstanceOf[InternalRow]
  }

  test("rule suite processing") {
    val rsr = RuleSuiteResult(
      Id(100,0), Passed, Map(
        Id(1,0) ->
          RuleSetResult(DisabledRule, Map(
            Id(1,0) -> Failed,
            Id(2,0) -> Passed,
            Id(3,0) -> IgnoredRule,
            Id(4,0) -> DefaultRule,
            Id(5,0) -> SoftFailed,
            Id(6,0) -> DisabledRule
          ))
      ))

    val og = rgStatsSer.eval(InternalRow(RuleSuiteGroupStatistics())).asInstanceOf[InternalRow]

    val rs = row(rsr)
    val res = StatsRowOps.processResult(og.copy(), rs)

    val resReal = rgStatsDer.eval(res).asInstanceOf[RuleSuiteGroupStatistics]

    verifySingleRuleSet(resReal)
    verifySingleRuleSet(RuleSuiteGroupStatistics() process rsr) // simpler functional code should be the same
    verifyCombination(og, RuleSuiteGroupStatistics(), res, resReal) // nothing present, full structural change

    val rsr2 = RuleSuiteResult(
      Id(100,0), Passed, Map(
        Id(1,0) ->
          RuleSetResult(DisabledRule, Map(
            Id(4,0) -> DefaultRule,
            Id(6,0) -> Passed,
            Id(7,0) -> Probability(0.4),
            Id(8,0) -> Probability(0.9)
          ))
      ))

    val ores = res.copy()
    val res2 = StatsRowOps.processResult(res, row(rsr2))
    val ores2 = res2.copy()

    val res_no_structural = StatsRowOps.processResult(res2.copy(), row(rsr2))

    val resReal2_no_structural = rgStatsDer.eval(res_no_structural).asInstanceOf[RuleSuiteGroupStatistics]
    resReal2_no_structural.rowCount shouldBe 3

    val resReal2 = rgStatsDer.eval(res2).asInstanceOf[RuleSuiteGroupStatistics]

    verifyExtendingASingleSet(resReal2)
    verifyExtendingASingleSet(resReal process rsr2)

    verifyCombination(ores, resReal, res2, resReal2)
    verifyCombination(res2, resReal2, res2, resReal2) // ensure no structural changes
    verifyCombination(og, RuleSuiteGroupStatistics(), res2, resReal2) // nothing present, full structural change

    val rsr3 = RuleSuiteResult(
      Id(100,0), Passed, Map(
        Id(2,0) ->
          RuleSetResult(DisabledRule, Map(
            Id(10,0) -> Passed,
            Id(11,0) -> Probability(0.4),
            Id(12,0) -> Probability(0.9)
          ))
      ))

    val res3 = StatsRowOps.processResult(res2, row(rsr3))
    val ores3 = res3.copy()

    val resReal3 = rgStatsDer.eval(res3).asInstanceOf[RuleSuiteGroupStatistics]

    verifyAddingARuleSet(resReal3)
    verifyAddingARuleSet(resReal2 process rsr3)

    verifyCombination(ores2, resReal2, res3, resReal3)
    verifyCombination(ores, resReal, res3, resReal3)
    verifyCombination(res3, resReal3, ores, resReal)
    verifyCombination(ores2, resReal2, ores, resReal)
    verifyCombination(res3, resReal3, ores2, resReal2)
    verifyCombination(res3, resReal3, res3, resReal3) // ensure no structural changes
    verifyCombination(og, RuleSuiteGroupStatistics(), ores3, resReal3) // nothing present, full structural change

    val rsr4 = RuleSuiteResult(
      Id(10,0), Passed, Map(
        Id(5,0) ->
          RuleSetResult(DisabledRule, Map(
            Id(10,0) -> Passed
          ))
      ))

    val res4 = StatsRowOps.processResult(res3, row(rsr4))

    val resReal4 = rgStatsDer.eval(res4).asInstanceOf[RuleSuiteGroupStatistics]

    verifyAddingARuleSuite(resReal4)
    verifyAddingARuleSuite(resReal3 process rsr4)

    verifyCombination(ores3, resReal3, res4, resReal4)
    verifyCombination(res4, resReal4, ores3, resReal3)
    verifyCombination(ores2, resReal2, res4, resReal4)
    verifyCombination(res4, resReal4, ores, resReal)
    verifyCombination(res4, resReal4, ores2, resReal2)
    verifyCombination(res4, resReal4, res4, resReal4) // ensure no structural changes
    verifyCombination(og, RuleSuiteGroupStatistics(), res4, resReal4) // nothing present, full structural change
  }

  private def verifyCombination(res: InternalRow, resReal: RuleSuiteGroupStatistics, res2: InternalRow, resReal2: RuleSuiteGroupStatistics) = {
    rgStatsDer.eval(StatsRowOps.combineResult(res.copy(), res2)).asInstanceOf[RuleSuiteGroupStatistics] shouldBe (resReal combine resReal2)
  }

  private def verifyAddingARuleSuite(resReal4: RuleSuiteGroupStatistics) = {
    resReal4.rowCount shouldBe 4
    resReal4.ruleSuites.size shouldBe 2
    resReal4.ruleSuites(Id(100, 0)).rowCount shouldBe 3
    resReal4.ruleSuites(Id(100, 0)).ruleSuite shouldBe Id(100, 0)
    resReal4.ruleSuites(Id(100, 0)).ruleSets.size shouldBe 2
    resReal4.ruleSuites(Id(100, 0)).ruleSets(Id(1, 0)).rules.size shouldBe 8
    resReal4.ruleSuites(Id(100, 0)).ruleSets(Id(1, 0)).rules(Id(6, 0)).disabled shouldBe 1
    resReal4.ruleSuites(Id(100, 0)).ruleSets(Id(1, 0)).rules(Id(6, 0)).passed shouldBe 1
    resReal4.ruleSuites(Id(100, 0)).ruleSets(Id(2, 0)).rules.size shouldBe 3
    resReal4.ruleSuites(Id(100, 0)).ruleSets(Id(2, 0)).rules(Id(10, 0)).disabled shouldBe 0
    resReal4.ruleSuites(Id(100, 0)).ruleSets(Id(2, 0)).rules(Id(10, 0)).passed shouldBe 1
    resReal4.ruleSuites(Id(10, 0)).rowCount shouldBe 1
    resReal4.ruleSuites(Id(10, 0)).ruleSuite shouldBe Id(10, 0)
    resReal4.ruleSuites(Id(10, 0)).ruleSets.size shouldBe 1
    resReal4.ruleSuites(Id(10, 0)).ruleSets(Id(5, 0)).rules.size shouldBe 1
    resReal4.ruleSuites(Id(10, 0)).ruleSets(Id(5, 0)).rules(Id(10, 0)).disabled shouldBe 0
    resReal4.ruleSuites(Id(10, 0)).ruleSets(Id(5, 0)).rules(Id(10, 0)).passed shouldBe 1
  }

  private def verifyAddingARuleSet(resReal3: RuleSuiteGroupStatistics) = {
    resReal3.rowCount shouldBe 3
    resReal3.ruleSuites.size shouldBe 1
    resReal3.ruleSuites(Id(100, 0)).rowCount shouldBe 3
    resReal3.ruleSuites(Id(100, 0)).ruleSuite shouldBe Id(100, 0)
    resReal3.ruleSuites(Id(100, 0)).ruleSets.size shouldBe 2
    resReal3.ruleSuites(Id(100, 0)).ruleSets(Id(1, 0)).rules.size shouldBe 8
    resReal3.ruleSuites(Id(100, 0)).ruleSets(Id(1, 0)).rules(Id(6, 0)).disabled shouldBe 1
    resReal3.ruleSuites(Id(100, 0)).ruleSets(Id(1, 0)).rules(Id(6, 0)).passed shouldBe 1
    resReal3.ruleSuites(Id(100, 0)).ruleSets(Id(2, 0)).rules.size shouldBe 3
    resReal3.ruleSuites(Id(100, 0)).ruleSets(Id(2, 0)).rules(Id(10, 0)).disabled shouldBe 0
    resReal3.ruleSuites(Id(100, 0)).ruleSets(Id(2, 0)).rules(Id(10, 0)).passed shouldBe 1
  }

  private def verifyExtendingASingleSet(resReal2: RuleSuiteGroupStatistics) = {
    resReal2.rowCount shouldBe 2
    resReal2.ruleSuites.size shouldBe 1
    resReal2.ruleSuites.head._2.rowCount shouldBe 2
    resReal2.ruleSuites.head._1 shouldBe Id(100, 0)
    resReal2.ruleSuites.head._2.ruleSets.size shouldBe 1
    resReal2.ruleSuites.head._2.ruleSets.head._2.rules.size shouldBe 8
    resReal2.ruleSuites.head._2.ruleSets.head._2.rules(Id(6, 0)).disabled shouldBe 1
    resReal2.ruleSuites.head._2.ruleSets.head._2.rules(Id(6, 0)).passed shouldBe 1
    resReal2.ruleSuites.head._2.ruleSets.head._2.rules(Id(4, 0)).defaulted shouldBe 2
  }

  private def verifySingleRuleSet(resReal: RuleSuiteGroupStatistics) = {
    resReal.rowCount shouldBe 1
    resReal.ruleSuites.size shouldBe 1
    resReal.ruleSuites.head._2.rowCount shouldBe 1
    resReal.ruleSuites.head._1 shouldBe Id(100, 0) // normal StatisticsTest covers the overall correctness
    resReal.ruleSuites.head._2.ruleSets.size shouldBe 1
    resReal.ruleSuites.head._2.ruleSets.head._2.rules.size shouldBe 6
    resReal.ruleSuites.head._2.ruleSets.head._2.rules(Id(6, 0)).disabled shouldBe 1
  }
}
