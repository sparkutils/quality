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
            Id(6,0) -> DisabledRule,
            Id(7,0) -> Probability(0.4),
            Id(8,0) -> Probability(0.9)
          ))
      ))

    val og = rgStatsSer.eval(InternalRow(RuleSuiteGroupStatistics())).asInstanceOf[InternalRow]
    val rs = rsResultSer.eval(InternalRow(rsr)).asInstanceOf[InternalRow]
    val res = StatsRowOps.processResult(og, rs)

    val resReal = rgStatsDer.eval(res).asInstanceOf[RuleSuiteGroupStatistics]
    resReal.rowCount shouldBe 1
  }
}
