package com.sparkutils.qualityTests

import org.apache.spark.sql.functions.{col, expr, udaf}
import com.sparkutils.quality.{RuleSuiteGroupStatistics, _}
import com.sparkutils.quality.functions.rule_suite_statistics
import com.sparkutils.quality.impl.aggregates.Statistics
import org.junit.Test
import org.scalatest.{FunSuite, Matchers}
import frameless.{Injection, NotCatalystNullable, TypedColumn, TypedEncoder, TypedExpressionEncoder}

class StatisticsTest  extends FunSuite with TestUtils with Matchers {

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

  @Test
  def setLevelOperations(): Unit = {
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
      disabled = 4, probabilityFailed = 3, probabilityPassed = 3, rules = updated.rules.updatedWith(Id(8,0)) {
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

  @Test
  def ruleSuiteOperations(): Unit = {
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

  @Test
  def ruleSuiteGroupOperations(): Unit = {
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

  // the above tests cover the actual functionality, outside empty process calls, the below are testing the expressions all work
  // and, given zero, process and combine all work from nothing
  @Test
  def udafAndEmptyProcessOperations(): Unit = {
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
    import sparkSession.implicits.localSeqToDatasetHolder
    import com.sparkutils.quality.implicits._
    import frameless._

    registerQualityFunctions()

    implicit val enc = TypedExpressionEncoder[(String, RuleSuiteResult)]

    val df = localSeqToDatasetHolder[(String, RuleSuiteResult)](rsr).toDS()
    val res = df.agg(rule_suite_statistics(col("_2")).as("res")).select("res.*").as[RuleSuiteGroupStatistics].collect().head

    val res2 = df.agg(expr("rule_suite_statistics(_2)").as("res")).select("res.*").as[RuleSuiteGroupStatistics].collect().head

    res shouldBe res2

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
