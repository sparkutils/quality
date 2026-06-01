package com.sparkutils.qualityTests

import com.sparkutils.quality.{ExpressionRule, Id, OutputExpression, RuleSuite, RunOnPassProcessor, groupProcessorKey, groupProcessorPercentFilter, ruleEngineRunner, showGroupingTime, showSplitCompilationTime, topLevelBooleanGrouper}
import com.sparkutils.qualityTests.util.SharedPureConnectTests
import org.apache.spark.sql.Column
import org.scalatest.Matchers

import scala.collection.JavaConverters._

class BooleanGrouperTest extends SharedPureConnectTests with Matchers {

  val testSize = 1000 // 9k takes almost 12m on the Ryzen ai 9 hx 370

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
    val d = data
    import d.sparkSession.implicits._
    val rules = RuleEngineTest.rulesRaw(
      (for { i <- 0 until 9000 } yield
        Seq(
          (ExpressionRule(s"(((a + b) % 20) < 5) and (a = $i)"), RunOnPassProcessor(1001, Id(9000+i, 1),
            OutputExpression("named_struct('y', a + b + c + d, 'z', a)"))),
          (ExpressionRule(s"(((a + b) % 20) < 15) and (a = $i)"), RunOnPassProcessor(1002, Id(90000+i, 1),
            OutputExpression("named_struct('y', a + b + c, 'z', a)"))),
          (ExpressionRule(s"(((a + b) % 20) < 20) and (a = $i)"), RunOnPassProcessor(1003, Id(900000+i, 1),
            OutputExpression("named_struct('y', a + b, 'z', a)")))
        )
    ).flatten)

    d.coalesce(1).withColumn("r", runner(rules).getField("result")).selectExpr(s"""
      case
       when ((a + b) % 20) < 5 then ((a + b + c + d) = r.y) and (r.z = a)
       when ((a + b) % 20) < 15 then ((a + b + c) = r.y) and (r.z = a)
       when ((a + b) % 20) < 20 then ((a + b) = r.y) and (r.z = a)
       else false
      end as passes
      """).filter("passes = false").count shouldBe 0
    /*as[(Int,Int,Int,Int,(Int,Int))].toLocalIterator().asScala.forall{
      case (a,b,c,d,(r, a2)) if (a + b) % 20 < 5 => a + b + c + d == r && a == a2
      case (a,b,c,d,(r, a2)) if (a + b) % 20 < 15 => a + b + c == r && a == a2
      case (a,b,c,d,(r, a2)) if (a + b) % 20 < 20 => a + b == r && a == a2
    }*/
  }

  // ensure that salience wins
  test("test grouping for overlapping ranges works 1:1"){ // 3.5ms per row with 3ms only in subexprs so 69s in total
    doTest(rs => ruleEngineRunner(rs, extraConfig = Map(
      showSplitCompilationTime -> "true",
      "statsEvery" -> "100"
    )))
  }

  test("test grouping for overlapping ranges works grouped"){ // 0.04ms per row 0.78s in total
    doTest(rs => ruleEngineRunner(rs, extraConfig = Map(
      groupProcessorKey -> topLevelBooleanGrouper,
      showSplitCompilationTime -> "true",
      showGroupingTime -> "true",
      "statsEvery" -> "100",
      // default fails to group properly as it's too intolerant, the differentiators are 3 occurrences in this test, the default is 23 or so
      groupProcessorPercentFilter -> "0.02"
    )))
  }
}
