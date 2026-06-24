package com.sparkutils.qualityTests.classicOnly

import com.sparkutils.quality._
import com.sparkutils.quality.impl.util.RuleSuiteGroupIOUtils
import com.sparkutils.qualityTests.RuleEngineTest
import com.sparkutils.qualityTests.util.{ClassicSharedTests, SharedPureConnectTests}
import com.sparkutils.testing.ConnectionType
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{Column, SaveMode}
import org.scalatest.Matchers

class BooleanGrouperTest extends ClassicSharedTests with Matchers {

  override val runWith: ConnectionType = com.sparkutils.testing.ClassicOnly

  val testSize = 1000 // 9k requires over 20gb ram to process, the trees are too large

  val rawData =
    for {
      i <- 0 until 1000
    }
    yield (i, i+1, i+2, i+5)

  def data = {
    val s = sparkSession
    import s.implicits._

    rawData.toDF("a","b","c","d")
  }

  /**
   * salience is lower for the lower numbers
   * @param runner
   * @return
   */
  def doTest(runner: RuleSuite => Column) = {
    // reading from 1 csv forces compilation on one executor
    data.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv(outputDir + "/bgt")
    val s = sparkSession
    val d = s.read.option("header", true).schema("a Int, b Int, c Int, d Int").csv(outputDir + "/bgt")
    val rules = RuleEngineTest.rulesRaw(
      (for { i <- 0 until testSize } yield
        Seq(
          (ExpressionRule(s"(((a + b) % 20) < 5) and (a = $i)"), RunOnPassProcessor(1001, Id(9000+i, 1),
            OutputExpression("named_struct('y', a + b + c + d, 'z', a)"))),
          (ExpressionRule(s"(((a + b) % 20) < 15) and (a = $i)"), RunOnPassProcessor(1002, Id(90000+i, 1),
            OutputExpression("named_struct('y', a + b + c, 'z', a)"))),
          (ExpressionRule(s"(((a + b) % 20) < 20) and (a = $i)"), RunOnPassProcessor(1003, Id(900000+i, 1),
            OutputExpression("named_struct('y', a + b, 'z', a)")))
        )
    ).flatten)

    d.select(expr("*"),runner(rules).getField("result").as("r")).select( expr(s"""
      case
       when ((a + b) % 20) < 5 then ((a + b + c + d) = r.y) and (r.z = a)
       when ((a + b) % 20) < 15 then ((a + b + c) = r.y) and (r.z = a)
       when ((a + b) % 20) < 20 then ((a + b) = r.y) and (r.z = a)
       else false
      end as passes
      """)).filter("passes = false").count shouldBe 0
    /*as[(Int,Int,Int,Int,(Int,Int))].toLocalIterator().asScala.forall{
      case (a,b,c,d,(r, a2)) if (a + b) % 20 < 5 => a + b + c + d == r && a == a2
      case (a,b,c,d,(r, a2)) if (a + b) % 20 < 15 => a + b + c == r && a == a2
      case (a,b,c,d,(r, a2)) if (a + b) % 20 < 20 => a + b == r && a == a2
    }*/
  }

  // ensure that salience wins
  test("test grouping for overlapping ranges works 1:1"){ not3_0_or_3_1 {// 3.5ms per row with 3ms only in subexprs so 69s in total
    doTest(rs => ruleEngineRunner(rs, extraConfig = Map(
      showSplitCompilationTime -> "true",
      "statsEvery" -> "100"
    )))
  } }

  test("test grouping for overlapping ranges works grouped") { not3_0_or_3_1 { // 0.025ms per row 0.5s in total
    doTest(rs => ruleEngineRunner(rs, extraConfig = Map(
      groupProcessorKey -> topLevelBooleanGrouper,
      showSplitCompilationTime -> "true",
      showGroupingTime -> "true",
      "statsEvery" -> "100",
      // default fails to group properly as it's too intolerant
      groupProcessorPercentFilter -> "0.1"
    )))
  } }

  // as this generates a differentiator of mod 8 for a the (a+b) mod's don't work.
  test("test grouping for overlapping ranges works grouped with too small buckets"){ not3_0_or_3_1 {// 0.04ms per row 0.78s in total
    doTest(rs => ruleEngineRunner(rs, extraConfig = Map(
      groupProcessorKey -> topLevelBooleanGrouper,
      showSplitCompilationTime -> "true",
      showGroupingTime -> "true",
      "statsEvery" -> "100",
      groupProcessorDumpAuditKey -> "true",
      groupProcessorAuditLocation -> outputDir,
      // default fails to group properly as it's too intolerant, the differentiators are 3 occurrences in this test, the default is 23 or so
      groupProcessorPercentFilter -> "0.15"
    )))

    val group = RuleSuiteGroupIOUtils.fromFile(outputDir + "/RuleEngineRunner")
    group.ruleSuites.size shouldBe 4

    // verify some of it is correct
    group.ruleSuites(Id(0,0)).ruleSets.exists(p => p.rules.exists(_.toString.contains("(((a + b) % 20) < 15)"))) shouldBe true
    group.ruleSuites.exists(_._2.ruleSets.exists(p => p.rules.exists(_.expression match {
      case hasRuleText: HasRuleText => hasRuleText.rule == "(a = 8)"
      case _ => false
    }))) shouldBe true
  } }

  // as it doesn't group it's a separate code path, which impact top level ctx vars as well due to predicate pushdown
  // you'll see all the time in this group, 0.44111285ms avg per row, 8.7s total
  // org.apache.spark.sql.catalyst.expressions.GeneratedClass$RunnerCompilationGroup0 - RunnerCompilationGroup0 avg 	48562300	14665000	 ns per every 	100	 rows
  test("test grouping for overlapping ranges works with bad groups"){ not3_0_or_3_1 {
    doTest(rs => ruleEngineRunner(rs, extraConfig = Map(
      groupProcessorKey -> topLevelBooleanGrouper,
      showSplitCompilationTime -> "true",
      showGroupingTime -> "true",
      "statsEvery" -> "100",
      groupProcessorDumpAuditKey -> "true",
      groupProcessorAuditLocation -> outputDir
      // default fails to group properly as it's too intolerant
    )))

    val group = RuleSuiteGroupIOUtils.fromFile(outputDir + "/RuleEngineRunner")
    group.ruleSuites.size shouldBe 2

    // we still have two rulesuites, its just forwarding only

    // verify some of it is correct
    group.ruleSuites(Id(0,0)).ruleSets.exists(p => p.rules.head.toString.contains("rule_engine_runner(rule_suite_from(the_group, 1, 0))")) shouldBe true
    group.ruleSuites(Id(1,0)).ruleSets.exists(p => p.rules.exists(_.toString.contains("((((a + b) % 20) < 15) AND (a = 0))"))) shouldBe true
  } }
}
