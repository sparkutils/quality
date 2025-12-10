package com.sparkutils.qualityTests

import com.sparkutils.quality.RuleSuite.mapRules
import com.sparkutils.qualityTests.util.SharedPureConnectTests
import com.sparkutils.quality._
import com.sparkutils.quality.functions.flatten_results
import com.sparkutils.quality.impl.util.{CombinedRuleSuiteRows, LambdaFunctionRow}
import com.sparkutils.qualityTests.RuleEngineTest.{rulesRaw, testData}
import com.sparkutils.testing.TestUtils.{anyCauseHas, debug}
import org.apache.spark.sql.ShimUtils
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.Matchers

class ConnectRuleSuitesTest extends SharedPureConnectTests with Matchers {

  val rsId = Id(1,1)
  val rules = RuleSuite(rsId, Seq(
    RuleSet(Id(50, 1), Seq(
      Rule(Id(100, 1), ExpressionRule("a"), RunOnPassProcessor(10, Id(101,1), OutputExpression("a"))),
      Rule(Id(100, 2), ExpressionRule("b"), RunOnPassProcessor(20, Id(101,2), OutputExpression("b"))),
      Rule(Id(100, 3), ExpressionRule("c"), RunOnPassProcessor(30, Id(101,3), OutputExpression("c"))),
      Rule(Id(100, 4), ExpressionRule("d"), RunOnPassProcessor(40, Id(101,4), OutputExpression("d")))
    )),
    RuleSet(Id(50, 2), Seq(
      Rule(Id(100, 5), ExpressionRule("e"), RunOnPassProcessor(50, Id(101,5), OutputExpression("e"))),
      Rule(Id(100, 6), ExpressionRule("f"), RunOnPassProcessor(60, Id(101,6), OutputExpression("f"))),
      Rule(Id(100, 7), ExpressionRule("g"), RunOnPassProcessor(70, Id(101,7), OutputExpression("g"))),
      Rule(Id(100, 8), ExpressionRule("h"), RunOnPassProcessor(80, Id(101,8), OutputExpression("h")))
    )),
    RuleSet(Id(50, 3), Seq(
      Rule(Id(100, 9),ExpressionRule("i"), RunOnPassProcessor(90, Id(101,9), OutputExpression("i"))),
      Rule(Id(100, 10), ExpressionRule("j"), RunOnPassProcessor(100, Id(101,10), OutputExpression("j"))),
      Rule(Id(100, 11), ExpressionRule("k"), RunOnPassProcessor(110, Id(101,11), OutputExpression("k"))),
      Rule(Id(100, 12), ExpressionRule("l"), RunOnPassProcessor(120, Id(101,12), OutputExpression("l")))
    ))
  ), Seq(
    LambdaFunction("func1", "expr1", Id(200,134)),
    LambdaFunction("func2", "expr2", Id(201,131))
  ))

  test("rule suites without lambdas or output should be combinable") {
    val stripped = mapRules(rules.copy(lambdaFunctions = Seq.empty)){_.copy(runOnPassProcessor = NoOpRunOnPassProcessor.noOp)}
    val ruleRows = toDS(stripped)
    val s = sparkSession
    import s.implicits._

    defaultAndForceConnect {
      val conbinedRuleSuiteRows = combine(ruleRows)
      val oRS = rule_suite(conbinedRuleSuiteRows, rsId)
      oRS contains stripped
    }
  }

  test("rule suites with lambdas no output should be combinable") {
    val stripped = mapRules(rules){_.copy(runOnPassProcessor = NoOpRunOnPassProcessor.noOp)}
    val ruleRows = toDS(stripped)
    val lambdas = toLambdaDS(stripped)
    val s = sparkSession
    import s.implicits._

    defaultAndForceConnect {
      val conbinedRuleSuiteRows = combine(ruleRows, lambdas)
      val oRS = rule_suite(conbinedRuleSuiteRows, rsId)
      oRS contains stripped
    }
  }

  test("rule suites without lambdas with output should be combinable") {
    val stripped = rules.copy(lambdaFunctions = Seq.empty)
    val ruleRows = toDS(stripped)
    val outRows = toOutputExpressionDS(stripped)

    val s = sparkSession
    import s.implicits._

    defaultAndForceConnect {
      val conbinedRuleSuiteRows = combine(ruleRows, sparkSession.emptyDataset[LambdaFunctionRow], outRows)
      val oRS = rule_suite(conbinedRuleSuiteRows, rsId)
      oRS contains stripped
    }
  }

  test("global libraries should properly integrate") {
    val s = sparkSession
    import s.implicits._

    val ruleRows = toDS(rules)
    // bump all the rulesuite ids out of whack, so they no longer align
    val lambdas = toLambdaDS(rules).collect().zipWithIndex.
      map{ case (l,i) => l.copy(ruleSuiteVersion = l.ruleSuiteVersion + i + 1, ruleSuiteId = l.ruleSuiteId + i + 1) }.toSeq
    val outRows = toOutputExpressionDS(rules).collect().zipWithIndex.
      map{ case (l,i) => l.copy(ruleSuiteVersion = l.ruleSuiteVersion + i + 1, ruleSuiteId = l.ruleSuiteId + i + 1) }.toSeq

    defaultAndForceConnect {
      // force them back in as global ids
      val conbinedRuleSuiteRows = combine(ruleRows, lambdas.toDS(), outRows.toDS(),
        globalLambdaSuites = lambdas.map(l => Id(l.ruleSuiteId, l.ruleSuiteVersion)).toDS(),
        globalOutputExpressionSuites = outRows.map(l => Id(l.ruleSuiteId, l.ruleSuiteVersion)).toDS())
      val oRS = rule_suite(conbinedRuleSuiteRows, rsId)
      oRS contains rules
    }
  }

  test("full rule suites should be combinable") {
    val ruleRows = toDS(rules)
    val lambdas = toLambdaDS(rules)
    val outRows = toOutputExpressionDS(rules)
    val s = sparkSession
    import s.implicits._

    defaultAndForceConnect {
      val conbinedRuleSuiteRows = combine(ruleRows, lambdas, outRows)
      val oRS = rule_suite(conbinedRuleSuiteRows, rsId)
      oRS contains rules
    }
  }

  test("rule suite spark var is convertible") {
    val ruleRows = toDS(rules)
    val lambdas = toLambdaDS(rules)
    val outRows = toOutputExpressionDS(rules)

    val s = sparkSession
    import s.implicits._

    defaultAndForceConnect {
      val conbinedRuleSuiteRows = combine(ruleRows, lambdas, outRows)
      val name = register_rule_suite_variable(conbinedRuleSuiteRows, rsId)
      val fromVar = sparkSession.sql(s"select `$name` as a").selectExpr("a.*").as[CombinedRuleSuiteRows]

      val oRS = rule_suite(fromVar, rsId)
      oRS contains rules
    }
  }

  test("ruleRunner via spark var and provided empty dataset") {
    val rules = rulesRaw(
      Seq((ExpressionRule("product = 'edt' and subcode = 40"), RunOnPassProcessor(1000, Id(1040, 1),
        OutputExpression("array(account_row('from'), account_row('to', 'other_account1'))"))),
        (ExpressionRule("product like '%fx%'"), RunOnPassProcessor(1000, Id(1042, 1),
          OutputExpression("array(named_struct('transfer_type', 'from', 'account', 'another_account', 'product', product, 'subcode', subcode), named_struct('transfer_type', 'to', 'account', account, 'product', product, 'subcode', subcode))"))),
        (ExpressionRule("product = 'eqotc'"), RunOnPassProcessor(1000, Id(1043, 1),
          OutputExpression("array(subcode('fromWithField', 6000), account_row('to', 'other_account1'))")))
      )
    )

    val testDataDF = {
      val s = sparkSession
      import s.implicits._
      testData.toDF()
    }

    import com.sparkutils.quality.implicits._

    val ruleRows = toDS(rules)
    val lambdas = toLambdaDS(rules)
    val outRows = toOutputExpressionDS(rules)

    val s = sparkSession
    import s.implicits._

    // the empty dataset but provided test
    defaultAndForceConnect {
      val conbinedRuleSuiteRows = combine(ruleRows, lambdas, outRows)
      val name = register_rule_suite_variable(conbinedRuleSuiteRows, rules.id)

      val outdf = testDataDF.withColumn("together",
        ShimUtils.callFunction("rule_engine_runner", col(name))
      )
      //outdf.show
      debug(outdf.select("together.*").show())
      val res = outdf.select("together.*").as[RuleEngineResult[Seq[NewPosting]]].collect()

      // this row will fail as the 0.6 doesn't class as a pass for the output expression - regardless of overall status
      assert(res(0).result.contains(Seq(NewPosting("from", "4201", "edt", 40), NewPosting("to", "other_account1", "edt", 40))))
      assert(res(0).salientRule.contains(SalientRule(Id(1, 1), Id(50, 1), Id(0, 1))))
      // TestOn("fx", "4206", 90),
      //    TestOn("fxotc", "4201", 40),
      assert(res(3).result.contains(Seq(NewPosting("from", "another_account", "fx", 90), NewPosting("to", "4206", "fx", 90))))
      assert(res(4).result.contains(Seq(NewPosting("from", "another_account", "fxotc", 40), NewPosting("to", "4201", "fxotc", 40))))
      assert(res(3).salientRule.contains(SalientRule(Id(1, 1), Id(50, 1), Id(100, 1))))
      assert(res(4).salientRule.contains(SalientRule(Id(1, 1), Id(50, 1), Id(100, 1))))

      // did the field replace work
      assert(res(5).result.contains(Seq(NewPosting("fromWithField", "4201", "eqotc", 6000), NewPosting("to", "other_account1", "eqotc", 60))))
    }
  }

  /**
   * Below from replaceWith test, the rest of the functionality is covered there
   */
  val struct = StructType(Seq(
    StructField("fielda", IntegerType)
  ))

  def doExpressionReplaceWith(ruleText: String, expected: String, rsf: RuleSuite => RuleSuite = identity) : Unit = {
    val orule = Rule(Id(2,1), ExpressionRule(ruleText))
    val rs = rsf(RuleSuite(Id(0,1), Seq(RuleSet(Id(1,1), Seq(orule)))))

    val cur = register_rule_suite(rs, "testRS")
    val nrs = process_if_attribute_missing(col(cur), struct, cur)

    val r = sparkSession.sql("select 2 fielda").selectExpr(s"dq_rule_runner($nrs) rr").
      select(explode(flatten_results(col("rr"))).as("expl")).selectExpr("expl.*").
      filter(s"ruleResult = $expected()").count()
    r shouldBe 1
  }

  test("testRuleDisableCoalesce") {
    val ruleText = "fieldb > 1"
    doExpressionReplaceWith(s"coalesceIfAttributesMissingDisable($ruleText)", "disabled_rule")
  }

  test("testRuleReplaceCoalesce") {
    val ruleText = "fieldb > 1"
    doExpressionReplaceWith(s"coalesceIfAttributesMissing($ruleText, false)", "failed")
  }

  test("testRuleNoReplaceCoalesce") {
    val ruleText = "fielda > 1"
    doExpressionReplaceWith(s"coalesceIfAttributesMissing($ruleText, 42)", "passed")
  }

  test("test process if with recursive rule") {
    val ruleText = "process_if_attribute_missing(testRS)"
    val caught =
      intercept[Exception] { // Result type: IndexOutOfBoundsException
        doExpressionReplaceWith(s"coalesceIfAttributesMissing($ruleText, 42)", "passed")
      }

    recursiveCheck(caught, "trigger rule")
  }

  test("test process if with recursive outputExpression") {
    val ruleText = "fielda > 1"
    val outputRule = "process_if_attribute_missing(testRS)"
    val caught =
      intercept[Exception] { // Result type: IndexOutOfBoundsException
        doExpressionReplaceWith(s"coalesceIfAttributesMissing($ruleText, 42)", "passed",
          mapRules(_){
            rule => rule.copy(runOnPassProcessor = RunOnPassProcessor(11, Id(13,13), OutputExpression(outputRule)))
          }
        )
      }

    recursiveCheck(caught, "output expression")
  }

  test("test process if with recursive lambda") {
    val ruleText = "fielda > 1"
    val outputRule = "process_if_attribute_missing(testRS)"
    val caught =
      intercept[Exception] { // Result type: IndexOutOfBoundsException
        doExpressionReplaceWith(s"coalesceIfAttributesMissing($ruleText, 42)", "passed",
          _.copy(lambdaFunctions = Seq(LambdaFunction("lam", outputRule, Id(13,13))))
        )
      }

    recursiveCheck(caught, "lambda function")
  }

  private def recursiveCheck(caught: Exception, typ: String) = {
    anyCauseHas(caught, {
      case q: Exception if // connect doesn't nest exceptions, they get put in the message
        q.getMessage.contains(s"process_if_attribute_missing should not be used") ||
          q.getMessage.contains(typ) => true
      case _ => false
    }) shouldBe true
  }
}
