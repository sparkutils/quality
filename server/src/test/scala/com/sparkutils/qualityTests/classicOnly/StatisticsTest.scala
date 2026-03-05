package com.sparkutils.qualityTests.classicOnly

import com.sparkutils.quality.ResultStatisticsProvider.ResultStatisticOps
import com.sparkutils.quality.impl.aggregates.StatsRowOps
import com.sparkutils.quality.{DefaultRule, DisabledRule, Failed, Id, IgnoredRule, Passed, Probability, RuleSetResult, RuleSuiteGroupStatistics, RuleSuiteResult, RuleSuiteStatistics, SoftFailed}
import com.sparkutils.qualityTests.util.ClassicSharedTests
import org.apache.spark.sql.ShimUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class StatisticsTest extends ClassicSharedTests with Matchers {

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
    val res = StatsRowOps.processResult(og, rs)

    val resReal = rgStatsDer.eval(res).asInstanceOf[RuleSuiteGroupStatistics]
    resReal.rowCount shouldBe 1
    resReal.ruleSuites.size shouldBe 1
    resReal.ruleSuites.head._2.rowCount shouldBe 1
    resReal.ruleSuites.head._1 shouldBe Id(100,0) // normal StatisticsTest covers the overall correctness
    resReal.ruleSuites.head._2.ruleSets.size shouldBe 1
    resReal.ruleSuites.head._2.ruleSets.head._2.rules.size shouldBe 6
    resReal.ruleSuites.head._2.ruleSets.head._2.rules(Id(6,0)).disabled shouldBe 1

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

    val res2 = StatsRowOps.processResult(res, row(rsr2))

    val resReal2 = rgStatsDer.eval(res2).asInstanceOf[RuleSuiteGroupStatistics]
    resReal2.rowCount shouldBe 2
    resReal2.ruleSuites.size shouldBe 1
    resReal2.ruleSuites.head._2.rowCount shouldBe 2
    resReal2.ruleSuites.head._1 shouldBe Id(100,0)
    resReal2.ruleSuites.head._2.ruleSets.size shouldBe 1
    resReal2.ruleSuites.head._2.ruleSets.head._2.rules.size shouldBe 8
    resReal2.ruleSuites.head._2.ruleSets.head._2.rules(Id(6,0)).disabled shouldBe 1
    resReal2.ruleSuites.head._2.ruleSets.head._2.rules(Id(6,0)).passed shouldBe 1
    resReal2.ruleSuites.head._2.ruleSets.head._2.rules(Id(4,0)).defaulted shouldBe 2

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

    val resReal3 = rgStatsDer.eval(res3).asInstanceOf[RuleSuiteGroupStatistics]
    resReal3.rowCount shouldBe 3
    resReal3.ruleSuites.size shouldBe 1
    resReal3.ruleSuites(Id(100,0)).rowCount shouldBe 3
    resReal3.ruleSuites(Id(100,0)).ruleSuite shouldBe Id(100,0)
    resReal3.ruleSuites(Id(100,0)).ruleSets.size shouldBe 2
    resReal3.ruleSuites(Id(100,0)).ruleSets(Id(1,0)).rules.size shouldBe 8
    resReal3.ruleSuites(Id(100,0)).ruleSets(Id(1,0)).rules(Id(6,0)).disabled shouldBe 1
    resReal3.ruleSuites(Id(100,0)).ruleSets(Id(1,0)).rules(Id(6,0)).passed shouldBe 1
    resReal3.ruleSuites(Id(100,0)).ruleSets(Id(2,0)).rules.size shouldBe 3
    resReal3.ruleSuites(Id(100,0)).ruleSets(Id(2,0)).rules(Id(10,0)).disabled shouldBe 0
    resReal3.ruleSuites(Id(100,0)).ruleSets(Id(2,0)).rules(Id(10,0)).passed shouldBe 1

    val rsr4 = RuleSuiteResult(
      Id(10,0), Passed, Map(
        Id(5,0) ->
          RuleSetResult(DisabledRule, Map(
            Id(10,0) -> Passed
          ))
      ))

    val res4 = StatsRowOps.processResult(res3, row(rsr4))

    val resReal4 = rgStatsDer.eval(res4).asInstanceOf[RuleSuiteGroupStatistics]
    resReal4.rowCount shouldBe 4
    resReal4.ruleSuites.size shouldBe 2
    resReal4.ruleSuites(Id(100,0)).rowCount shouldBe 3
    resReal4.ruleSuites(Id(100,0)).ruleSuite shouldBe Id(100,0)
    resReal4.ruleSuites(Id(100,0)).ruleSets.size shouldBe 2
    resReal4.ruleSuites(Id(100,0)).ruleSets(Id(1,0)).rules.size shouldBe 8
    resReal4.ruleSuites(Id(100,0)).ruleSets(Id(1,0)).rules(Id(6,0)).disabled shouldBe 1
    resReal4.ruleSuites(Id(100,0)).ruleSets(Id(1,0)).rules(Id(6,0)).passed shouldBe 1
    resReal4.ruleSuites(Id(100,0)).ruleSets(Id(2,0)).rules.size shouldBe 3
    resReal4.ruleSuites(Id(100,0)).ruleSets(Id(2,0)).rules(Id(10,0)).disabled shouldBe 0
    resReal4.ruleSuites(Id(100,0)).ruleSets(Id(2,0)).rules(Id(10,0)).passed shouldBe 1
    resReal4.ruleSuites(Id(10,0)).rowCount shouldBe 1
    resReal4.ruleSuites(Id(10,0)).ruleSuite shouldBe Id(10,0)
    resReal4.ruleSuites(Id(10,0)).ruleSets.size shouldBe 1
    resReal4.ruleSuites(Id(10,0)).ruleSets(Id(5,0)).rules.size shouldBe 1
    resReal4.ruleSuites(Id(10,0)).ruleSets(Id(5,0)).rules(Id(10,0)).disabled shouldBe 0
    resReal4.ruleSuites(Id(10,0)).ruleSets(Id(5,0)).rules(Id(10,0)).passed shouldBe 1

  }
}
