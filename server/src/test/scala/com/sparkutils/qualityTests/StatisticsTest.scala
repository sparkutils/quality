package com.sparkutils.qualityTests

import org.apache.spark.sql.functions.{col, expr}
import com.sparkutils.quality.{RuleSuiteGroupStatistics, _}
import com.sparkutils.quality.functions.{rule_suite_statistics, rule_suite_statistics_aggregator}
import org.scalatest.Matchers
import com.sparkutils.qualityTests.util.SharedPureConnectTests
import com.sparkutils.quality.ResultStatisticsProvider.ResultStatisticOps
import org.apache.spark.sql.Column

class StatisticsTest extends SharedPureConnectTests with Matchers {

  // the implementation does not use the above logic but the above tests form the backbone of the classicOnly tests
  test("default declarative stats function") {
    aggTest("rule_suite_statistics", rule_suite_statistics)
  }

  // #117 - shouldn't be used, but kept in case the default implementation has issues, this is easier to prove correctness
  // and far easier to reason about
  test("aggregator based stats function") { not_Databricks {
    aggTest("rule_suite_statistics_aggregator", rule_suite_statistics_aggregator)
  } }

  def aggTest(func: String, statF: Column => Column): Unit = {
    val rsr = Seq(
      ("a", RuleSuiteResult(
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
        ))),
      ("a", RuleSuiteResult(
        Id(100,0), IgnoredRule, Map(
          Id(1,0) ->
            RuleSetResult(DefaultRule, Map(
              Id(1,0) -> Passed,
              Id(2,0) -> SoftFailed,
              Id(3,0) -> IgnoredRule,
              Id(4,0) -> DefaultRule,
              Id(5,0) -> SoftFailed,
              Id(6,0) -> DisabledRule,
              Id(7,0) -> Probability(0.4),
              Id(8,0) -> Probability(0.9)
            ))
        ))),
      ("a", RuleSuiteResult(
        Id(200,0), Failed, Map(
          Id(10,0) ->
            RuleSetResult(Probability(0.9), Map(
              Id(10,0) -> Failed,
              Id(20,0) -> Passed,
              Id(30,0) -> IgnoredRule,
              Id(40,0) -> DefaultRule,
              Id(50,0) -> SoftFailed,
              Id(60,0) -> DisabledRule,
              Id(70,0) -> Probability(0.4),
              Id(80,0) -> Probability(0.9)
            ))
        )))
    )
    val s = sparkSession
    import s.implicits.localSeqToDatasetHolder
    import com.sparkutils.quality.implicits._
    import frameless._

    registerQualityFunctions()

    implicit val enc = TypedExpressionEncoder[(String, RuleSuiteResult)]

    val df = localSeqToDatasetHolder[(String, RuleSuiteResult)](rsr).toDS()
    //df.show()

    val tmp = df.select(statF(col("_2")).as("res")).select("res.*")
    val sch = tmp.schema
    //tmp.show()

    val res = tmp.as[RuleSuiteGroupStatistics].collect().head

    val res2 = df.select(expr(s"$func(_2)").as("res")).select("res.*").as[RuleSuiteGroupStatistics].collect().head

    // verify split
    val res3 = df.selectExpr( s"_2.overallResult as overallResult",
        s"ruleSuiteResultDetails(_2) as resultDetails").
      select(expr(s"$func(struct(resultDetails.id, overallResult, resultDetails.ruleSetResults))").as("res")).select("res.*").as[RuleSuiteGroupStatistics].collect().head

    res shouldBe res2
    res shouldBe res3

    res shouldBe
      RuleSuiteGroupStatistics(
        Map(
          Id(100,0) ->
            RuleSuiteStatistics(Id(100,0), passed = 1, ignored = 1,
              ruleSets =
                Map(
                  Id(1, 0) ->
                    RuleSetStatistics(Id(1,0), disabled = 1, defaulted = 1,
                      rules = Map(
                        Id(1,0) -> RuleStatistics(Id(1,0), failed = 1, passed = 1),
                        Id(2,0) -> RuleStatistics(Id(2,0), passed = 1, softFailed = 1),
                        Id(3,0) -> RuleStatistics(Id(3,0), ignored = 2),
                        Id(4,0) -> RuleStatistics(Id(4,0), defaulted = 2),
                        Id(5,0) -> RuleStatistics(Id(5,0), softFailed = 2),
                        Id(6,0) -> RuleStatistics(Id(6,0), disabled = 2),
                        Id(7,0) -> RuleStatistics(Id(7,0), probabilityFailed = 2),
                        Id(8,0) -> RuleStatistics(Id(8,0), probabilityPassed = 2)
                      ))
                ),
              rowCount = 2
            ),
          Id(200,0) ->
            RuleSuiteStatistics(Id(200,0), failed = 1,
              ruleSets =
                Map(
                  Id(10, 0) ->
                    RuleSetStatistics(Id(10,0), probabilityPassed = 1,
                      rules = Map(
                        Id(10,0) -> RuleStatistics(Id(10,0), failed = 1),
                        Id(20,0) -> RuleStatistics(Id(20,0), passed = 1),
                        Id(30,0) -> RuleStatistics(Id(30,0), ignored = 1),
                        Id(40,0) -> RuleStatistics(Id(40,0), defaulted = 1),
                        Id(50,0) -> RuleStatistics(Id(50,0), softFailed = 1),
                        Id(60,0) -> RuleStatistics(Id(60,0), disabled = 1),
                        Id(70,0) -> RuleStatistics(Id(70,0), probabilityFailed = 1),
                        Id(80,0) -> RuleStatistics(Id(80,0), probabilityPassed = 1)
                      ))
                ),
              rowCount = 1
            )
        ),
        rowCount = 3)

  }
}
