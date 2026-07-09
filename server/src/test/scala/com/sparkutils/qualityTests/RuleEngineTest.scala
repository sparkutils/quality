package com.sparkutils.qualityTests

import com.sparkutils.quality._
import com.sparkutils.quality.functions.{flatten_rule_results, unpack_id_triple}
import com.sparkutils.quality.impl.{OverallResult, RuleSuiteHelpers, Runners}
import com.sparkutils.qualityTests.RuleEngineTest.{rulesRaw, testData}
import com.sparkutils.qualityTests.util.SharedPureConnectTests
import com.sparkutils.testing.TestUtils.debug
import org.apache.spark.sql.{DataFrame, ShimUtils, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.Matchers

case class TestOn(product: String, account: String, subcode: Int)

case class NewPosting(transfer_type: String, account: String, product: String, subcode: Int)
case class Posting(transfer_type: String, account: String)

object RuleEngineTest {

  val testData=Seq(
    TestOn("edt", "4201", 40),
    TestOn("otc", "5201", 40),
    TestOn("fi", "4251", 50),
    TestOn("fx", "4206", 90),
    TestOn("fxotc", "4201", 40),
    TestOn("eqotc", "4201", 60)
  )

  val DDL = "ARRAY<STRUCT<`transfer_type`: STRING, `account`: STRING, `product`: STRING, `subcode`: INTEGER >>"

  def rulesRaw(expressionRules: Seq[(ExpressionRule, RunOnPassProcessor)]) = {
    registerLambdaFunctions(Seq(
      LambdaFunction("account_row", "(transfer_type, account) -> named_struct('transfer_type', transfer_type, 'account', account, 'product', product, 'subcode', subcode)", Id(123, 23)),
      LambdaFunction("account_row", "transfer_type -> account_row(transfer_type, account)", Id(123, 24)),
      LambdaFunction("subcode", "(transfer_type, sub) -> updateField(account_row(transfer_type, account), 'subcode', sub)", Id(123, 25))
    ))

    val rules =
      for { ((exp, processor), idOffset) <- expressionRules.zipWithIndex }
        yield Rule(Id(100 * idOffset, 1), exp, processor)

    val rsId = Id(1, 1)
    val ruleSuite = RuleSuite(rsId, Seq(
      RuleSet(Id(50, 1), rules
      )))

    ruleSuite
  }

}

trait RuleEngineTestBase extends SharedPureConnectTests with Matchers {

  def debugRules(expressionRules: (ExpressionRule, RunOnPassProcessor) *) =
    irules(expressionRules, true)

  def rules(expressionRules: (ExpressionRule, RunOnPassProcessor) *) =
    irules(expressionRules)

  def irules(expressionRules: Seq[(ExpressionRule, RunOnPassProcessor)], debugMode: Boolean = false, compileEvals: Boolean = true, transformRuleSuite: RuleSuite => RuleSuite = identity) = {
    val ruleSuite = rulesRaw(expressionRules)
    if (ShimUtils.isClassic(SparkSession.active))
      (dataFrame: DataFrame) =>
        Runners.ruleEngineRunner(transformRuleSuite(ruleSuite), debugMode = debugMode,
          resolveWith = if (doResolve.get()) Some(dataFrame) else None, compileEvals = compileEvals).get
    else
      (_: DataFrame) =>
        ShimUtils.callFunction("rule_engine_runner", lit(RuleSuiteHelpers.serialize(transformRuleSuite(ruleSuite))),
          lit(""), lit(debugMode)
        )
  }

  def doTestProbabilityRules(overallResult: OverallResult): Unit = evalCodeGens {
    val rer = irules(
      Seq((ExpressionRule("0.6"), RunOnPassProcessor(1000, Id(1040,1),
        OutputExpression("array(account_row('from'), account_row('to', 'other_account1'))"))))
      , transformRuleSuite = _.withProbablePass(overallResult.probablePass))

    val testDataDF = {
      val s = sparkSession
      import s.implicits._
      testData.toDF()
    }

    import com.sparkutils.quality.implicits._

    val outdf = testDataDF.withColumn("together", rer(testDataDF))

    val res = outdf.select("together.*").as[RuleEngineResult[Seq[Posting]]].collect()
    assert(res(0).result.isEmpty)
    assert(res(0).salientRule.isEmpty)
    assert(res(0).ruleSuiteResults.overallResult == overallResult.currentResult)
  }

  def doSimpleProductionRules(): Unit = {
    val rer = irules(
      Seq((ExpressionRule("product = 'edt' and subcode = 40"), RunOnPassProcessor(1000, Id(1040, 1),
        OutputExpression("array(account_row('from'), account_row('to', 'other_account1'))"))),
        (ExpressionRule("product like '%fx%'"), RunOnPassProcessor(1001, Id(1042, 1),
          OutputExpression("array(named_struct('transfer_type', 'from', 'account', 'another_account', 'product', product, 'subcode', subcode), named_struct('transfer_type', 'to', 'account', account, 'product', product, 'subcode', subcode))"))),
        (ExpressionRule("product = 'eqotc'"), RunOnPassProcessor(1002, Id(1043, 1),
          OutputExpression("array(subcode('fromWithField', 6000), account_row('to', 'other_account1'))")))
      ), compileEvals = false
    )

    val testDataDF = {
      val s = sparkSession
      import s.implicits._
      testData.toDF()
    }

    import com.sparkutils.quality.implicits._
    defaultAndForceConnect {
      val outdf = testDataDF.withColumn("together", rer(testDataDF))
      //outdf.show
      debug(outdf.select("together.*").show())
      val res = outdf.select("together.*").as[RuleEngineResult[Seq[NewPosting]]].collect()
      // #112 - overall should make sense
      val (passed, failed) = res.zipWithIndex.partition {
        _._1.ruleSuiteResults.overallResult == Passed
      }
      passed.map(_._2) shouldBe Seq(0, 3, 4, 5)
      failed.map(_._2) shouldBe Seq(1, 2)

      // this row will fail as the 0.6 doesn't class as a pass for the output expression - regardless of overall status
      assert(res(0).result.contains(Seq(NewPosting("from", "4201", "edt", 40), NewPosting("to", "other_account1", "edt", 40))))
      assert(res(0).salientRule.contains(SalientRule(Id(1, 1), Id(50, 1), Id(0, 1))))
      // #128 - the other rule should be Unevaluated
      val rr0 = res(0).ruleSuiteResults.ruleSetResults(Id(50,1)).ruleResults
      val rr0r = Seq(rr0(Id(100,1)), rr0(Id(200,1)))
      v3_5_and_above { // spark 3/3.1 don't actually respect the compilation flag
        if (inCodegen) {
          rr0r shouldBe Seq(UnevaluatedRule, UnevaluatedRule)
        } else {
          rr0r shouldBe Seq(Failed, Failed)
        }
      }

      // TestOn("fx", "4206", 90),
      //    TestOn("fxotc", "4201", 40),
      assert(res(3).result.contains(Seq(NewPosting("from", "another_account", "fx", 90), NewPosting("to", "4206", "fx", 90))))
      assert(res(4).result.contains(Seq(NewPosting("from", "another_account", "fxotc", 40), NewPosting("to", "4201", "fxotc", 40))))
      assert(res(3).salientRule.contains(SalientRule(Id(1, 1), Id(50, 1), Id(100, 1))))
      assert(res(4).salientRule.contains(SalientRule(Id(1, 1), Id(50, 1), Id(100, 1))))
      // #128 - the other rule should be Unevaluated
      def rr34(i: Int) = {
        val rr1 = res(i).ruleSuiteResults.ruleSetResults(Id(50, 1)).ruleResults
        val rr1r = Seq(rr1(Id(0, 1)), rr1(Id(200, 1)))
        if (inCodegen) {
          rr1r shouldBe Seq(Failed, UnevaluatedRule)
        } else {
          rr1r shouldBe Seq(Failed, Failed)
        }
      }
      v3_5_and_above { // spark 3/3.1 don't actually respect the compilation flag
        rr34(3)
        rr34(4)
      }

      // did the field replace work
      assert(res(5).result.contains(Seq(NewPosting("fromWithField", "4201", "eqotc", 6000), NewPosting("to", "other_account1", "eqotc", 60))))
      // #128 - the other rule should be Unevaluated
      val rr2 = res(5).ruleSuiteResults.ruleSetResults(Id(50,1)).ruleResults
      val rr2r = Seq(rr2(Id(0,1)), rr2(Id(100,1)))
      v3_5_and_above { // spark 3/3.1 don't actually respect the compilation flag
        if (inCodegen) {
          rr2r shouldBe Seq(Failed, Failed)
        } else {
          rr2r shouldBe Seq(Failed, Failed)
        }
      }
    }
  }

  def doTestFlattenResults(): Unit =  {
    val rer = rules(
      (ExpressionRule("product = 'edt' and subcode = 40"), RunOnPassProcessor(1000, Id(1040,1),
        OutputExpression("array(account_row('from', account), account_row('to', 'other_account1'))"))),
      (ExpressionRule("product like '%fx%'"), RunOnPassProcessor(1001, Id(1041,1),
        OutputExpression("array(named_struct('transfer_type', 'from', 'account', 'another_account', 'product', product, 'subcode', subcode), named_struct('transfer_type', 'to', 'account', account, 'product', product, 'subcode', subcode))")))
    )

    val s = sparkSession
    import s.implicits._

    val testDataDF = testData.toDF()

    val interimT = testDataDF.withColumn("together", rer(testDataDF)).cache()
    val outdfi = interimT.selectExpr("explode(flattenRuleResults(together)) as expl")
    val outdfi2 = interimT.select(explode(flatten_rule_results(col("together"))) as "expl")
    assert(outdfi.union(outdfi2).distinct().count() == outdfi.distinct().count())

    debug {
      println("outdfi show")

      outdfi.show()
      outdfi.printSchema()
    }

    val interim = outdfi.selectExpr("expl.result")
    debug {
      interim.printSchema()
      interim.show()
    }

    val res = interim.as[Seq[Posting]].collect()
    assert(res(0) == Seq(Posting("from", "4201"), Posting("to","other_account1")))
    assert(res(6) == Seq(Posting("from", "another_account"), Posting("to","4206")))
    assert(res(8) == Seq(Posting("from", "another_account"), Posting("to","4201")))
  }

  def doTestSalience(): Unit = {
    val rer = rules(
      (ExpressionRule("product = 'eqotc' and account = '4201'"), RunOnPassProcessor(100, Id(1040,1),
        OutputExpression("array(updateField(account_row('fr', account), 'transfer_type', 'from'), account_row('to', 'other_account1'))"))),
      (ExpressionRule("account = '4201'"), RunOnPassProcessor(1000, Id(1041,1),
        OutputExpression("array(named_struct('transfer_type', 'from', 'account', 'another_account', 'product', product, 'subcode', subcode), named_struct('transfer_type', 'to', 'account', account, 'product', product, 'subcode', subcode))")))
    )

    val testDataDF = {
      val s = sparkSession
      import s.implicits._
      testData.toDF()
    }

    // # 75 has issues with encoding on interpreted with dbr 15.4
    import frameless._

    val outdf = testDataDF.withColumn("together", rer(testDataDF)).selectExpr("*", "together.result")
    debug( outdf.show() )

    val res = outdf.select("result").as[Option[Seq[Posting]]](TypedExpressionEncoder[Option[Seq[Posting]]]).collect()
    val just4201 = Seq(Posting("from", "another_account"), Posting("to","4201"))
    assert(res(0).contains(just4201))
    assert(res(4).contains(just4201))
    assert(res(5).contains(Seq(Posting("from", "4201"), Posting("to", "other_account1"))))

    // prove unpackIdTriple works
    val srulec = outdf.select(unpack_id_triple(col("together.salientRule")) as "salientRule").selectExpr("salientRule.*")
    val srule = outdf.selectExpr("unpackIdTriple(together.salientRule) as salientRule").selectExpr("salientRule.*")
    assert(srule.union(srulec).distinct().count() == srule.distinct().count())

    // need Option for the int's because they may be null.
    val sruleres = srule.select("ruleSuiteId","ruleSuiteVersion","ruleSetId","ruleSetVersion","ruleId","ruleVersion").
      as[(Option[Int],Option[Int],Option[Int],Option[Int],Option[Int],Option[Int])](
        TypedExpressionEncoder[(Option[Int],Option[Int],Option[Int],Option[Int],Option[Int],Option[Int])]).collect()
    assert(sruleres(0) == (Some(1),Some(1),Some(50),Some(1),Some(100),Some(1)))
    // prove it's all nulls here i.e. salientRule is null if no rule matched
    val nulls = (None,None,None,None,None,None)
    assert(sruleres(1) == nulls)
    assert(sruleres(2) == nulls)
    assert(sruleres(3) == nulls)
  }

  def doTestDebug(): Unit = {
    val rer = debugRules(
      (ExpressionRule("product = 'eqotc' and account = '4201'"), RunOnPassProcessor(100, Id(1040,1),
        OutputExpression("array(account_row('from', account), account_row('to', 'other_account1'))"))),
      (ExpressionRule("account = '4201'"), RunOnPassProcessor(1000, Id(1041,1),
        OutputExpression("array(named_struct('transfer_type', 'from', 'account', 'another_account', 'product', product, 'subcode', subcode), named_struct('transfer_type', 'to', 'account', account, 'product', product, 'subcode', subcode))")))
    )

    val testDataDF = {
      val s = sparkSession
      import s.implicits._
      testData.toDF()
    }
    import frameless._

    val outdf = testDataDF.withColumn("together", rer(testDataDF)).selectExpr("*", "together.result")
    debug {
      outdf.show()
      outdf.printSchema()
    }

    val res = outdf.select("result").as[Option[Seq[(Int, Seq[Posting])]]](TypedExpressionEncoder[Option[Seq[(Int, Seq[Posting])]]]).collect()
    val just4201 = Seq(Posting("from", "another_account"), Posting("to","4201"))
    val justSeq = (1000, just4201)
    assert(res(0).contains(Seq(justSeq)))
    assert(res(4).contains(Seq(justSeq)))
    assert(res(5).contains(Seq((100, Seq(Posting("from", "4201"), Posting("to", "other_account1"))), justSeq)))
  }

}

class RuleEngineTest extends RuleEngineTestBase {

  test("testSimpleProductionRules Connect") { connectOnly{
    doSimpleProductionRules()
  } }

  test("testProbabilityRuleFail") { doTestProbabilityRules(OverallResult(currentResult = Failed)) }

  test("testProbabilityRulePass") { doTestProbabilityRules(OverallResult(probablePass = 0.6, currentResult = Passed)) }

  test("scalarSubqueryAsOutputExpressionInStruct") { evalCodeGensNoResolve {
    v3_4_and_above {
      // assert that using a join to test with is fine even when nested
      val s = sparkSession
      import s.implicits._

      val seq = Seq(0, 1, 2, 3, 4)
      val df = seq.toDF("i") // Force GenericArrayData instead of UnsafeArrayData
      df.write.mode("overwrite").parquet(outputDir + "/i_s_hav_it") // force relation as LocalRelation is driver only so no serialisation attempted
      val tableName = "the_I_s_Have_It"
      sparkSession.read.parquet(outputDir + "/i_s_hav_it").
        createOrReplaceTempView(tableName)

      // this won't work directly as it's not serializable, it must be a 'top-level' field.
      def sub(comp: String = "> 2", tableSuffix: String = "") = s"struct((select max(i_s$tableSuffix.i) from $tableName i_s$tableSuffix where i_s$tableSuffix.i $comp))"

      val rs = RuleSuite(Id(1, 1), Seq(
        RuleSet(Id(50, 1), Seq(
          Rule(Id(101, 1), ExpressionRule(s"(select max(i) > 1 from $tableName)"), RunOnPassProcessor(1000, Id(3010, 1),
            OutputExpression(sub("> main.i"))))
        ))
      ))
      val testDF = seq.toDF("i").as("main")
      testDF.collect()
      val resdf = testDF.transform(ruleEngineWithStructFOT(rs))
      try {
        val res = resdf.selectExpr("ruleEngine.result.col1").as[Option[Int]].collect()
        assert(res.count(_.isEmpty) == 1)
        assert(res.flatten.forall(_ == 4))
      } catch {
        case t: Throwable =>
          throw t
      }
    }
  } }

  test("scalarSubqueryAsOutputExpression") { evalCodeGensNoResolve {
    v3_4_and_above {
      // assert that using a join to test with is fine even when nested
      val s = sparkSession
      import s.implicits._

      val seq = Seq(0, 1, 2, 3, 4)
      val df = seq.toDF("i") // Force GenericArrayData instead of UnsafeArrayData
      val tableName = "the_I_s_Have_It"
      df.createOrReplaceTempView(tableName)

      // this won't work directly as it's not serializable, it must be a 'top-level' field.
      def sub(comp: String = "> 2", tableSuffix: String = "") = s"select max(i_s$tableSuffix.i) from $tableName i_s$tableSuffix where i_s$tableSuffix.i $comp"

      val rs = RuleSuite(Id(1, 1), Seq(
        RuleSet(Id(50, 1), Seq(
          Rule(Id(101, 1), ExpressionRule("true"), RunOnPassProcessor(1000, Id(3010, 1),
            OutputExpression(sub("> main.i"))))
        ))
      ))
      val testDF = seq.toDF("i")
      testDF.collect()
      val resdf = testDF.transform(ruleEngineWithStructFOT(rs))
      try {
        val res = resdf.selectExpr("ruleEngine.result").as[Option[Int]].collect()
        assert(res.count(_.isEmpty) == 1)
        assert(res.flatten.forall(_ == 4))
      } catch {
        case t: Throwable =>
          throw t
      }
    }
  } }

  test("scalarSubqueryAsOutputExpressionViaLambdaParam") { evalCodeGensNoResolve {
    v3_4_and_above {
      // using subqueries in lambdas does not work, it can't see the outer scope when it's a lambda variable, assume it's something like bind being called after subquery

      // assert that using a join to test with is fine even when nested
      val s = sparkSession
      import s.implicits._

      val seq = Seq(0, 1, 2, 3, 4)
      val df = seq.toDF("i") // Force GenericArrayData instead of UnsafeArrayData
      val tableName = "the_I_s_Have_It"
      df.createOrReplaceTempView(tableName)

      // the struct(( sub )).col1 'trick' allows parsing
      def sub(tableSuffix: String = "") = s"ii -> select named_struct('themax', max(i_s$tableSuffix.i), 'thedouble', max(i_s$tableSuffix.i) * 2) from $tableName i_s$tableSuffix where i_s$tableSuffix.i > ii"

      val rs = RuleSuite(Id(1, 1), Seq(
        RuleSet(Id(50, 1), Seq(
          Rule(Id(101, 1), ExpressionRule("true"), RunOnPassProcessor(1000, Id(3010, 1),
            OutputExpression("genMax(i).thedouble")))
        ))
      ), Seq(LambdaFunction("genMax", sub(), Id(2404,1))))
      val testDF = seq.toDF("i")
      testDF.collect()
      def testRes(resdf: DataFrame): Unit = {
        try {
          val res = resdf.selectExpr("ruleEngine.result").as[Option[Int]].collect()
          assert(res.count(_.isEmpty) == 1)
          assert(res.flatten.forall(_ == 8))
        } catch {
          case t: Throwable =>
            throw t
        }
      }

      // test no alias paths as well
      testRes(testDF.transform(ruleEngineWithStructFOT(rs, alias = null)).asInstanceOf[DataFrame])
      testRes(testDF.transform(ruleEngineWithStructFOT(rs, alias = "")).asInstanceOf[DataFrame])
    }
  } }

  test("scalarSubqueryAsOutputExpressionViaLambdaNonAttributeParam") { evalCodeGensNoResolve {
    v3_4_and_above {
      // assert that using a join to test with is fine even when nested
      val s = sparkSession
      import s.implicits._

      val seq = Seq(0, 1, 2, 3, 4)
      val df = seq.toDF("i") // Force GenericArrayData instead of UnsafeArrayData
      val tableName = "the_I_s_Have_It"
      df.createOrReplaceTempView(tableName)

      // the struct(( sub )).col1 'trick' allows parsing
      //      def sub(tableSuffix: String = "") = s"ii -> struct((select max(i_s$tableSuffix.i) from $tableName i_s$tableSuffix where i_s$tableSuffix.i > identity(ii))).col1"
      def sub(tableSuffix: String = "") = s"ii -> select max(i_s$tableSuffix.i) from $tableName i_s$tableSuffix where i_s$tableSuffix.i > ii"

      val rs = RuleSuite(Id(1, 1), Seq(
        RuleSet(Id(50, 1), Seq(
          Rule(Id(101, 1), ExpressionRule("true"), RunOnPassProcessor(1000, Id(3010, 1),
            OutputExpression("genMax(i * 1)")))
        ))
      ), Seq(LambdaFunction("genMax", sub(), Id(2404,1))))
      val testDF = seq.toDF("i").as("main")
      testDF.collect()
      val resdf = testDF.transform(ruleEngineWithStructFOT(rs))
      try {
        val res = resdf.selectExpr("ruleEngine.result").as[Option[Int]].collect()
        // the o.g. '4' value should return null
        assert(res.count(_.isEmpty) == 1)
        assert(res.flatten.forall(_ == 4))
      } catch {
        case t: Throwable =>
          throw t
      }
    }
  } }

  test("scalarSubqueryAsOutputExpressionViaLambdaNoParam") { evalCodeGensNoResolve {
    v3_4_and_above {
      // in this scenario the lambda is just used to avoid repeating the subquery, pretty much just a join.

      // assert that using a join to test with is fine even when nested
      val s = sparkSession
      import s.implicits._

      val seq = Seq(0, 1, 2, 3, 4)
      val df = seq.toDF("i") // Force GenericArrayData instead of UnsafeArrayData
      val tableName = "the_I_s_Have_It"
      df.createOrReplaceTempView(tableName)

      // the lambda is just an expression, main.i still needed to disambiguate or it silently fails
      def sub(tableSuffix: String = "") = s"select max(i_s$tableSuffix.i) from $tableName i_s$tableSuffix where i_s$tableSuffix.i > main.i"

      val rs = RuleSuite(Id(1, 1), Seq(
        RuleSet(Id(50, 1), Seq(
          Rule(Id(101, 1), ExpressionRule("true"), RunOnPassProcessor(1000, Id(3010, 1),
            OutputExpression("genMax()")))
        ))
      ), Seq(LambdaFunction("genMax", sub(), Id(2404,1))))
      val testDF = seq.toDF("i")
      testDF.collect()
      val resdf = testDF.transform(ruleEngineWithStructFOT(rs)) // uses main default
      try {
        val res = resdf.selectExpr("ruleEngine.result").as[Option[Int]].collect()
        // the o.g. '4' value should return null
        assert(res.count(_.isEmpty) == 1)
        assert(res.flatten.forall(_ == 4))
      } catch {
        case t: Throwable =>
          throw t
      }
    }
  } }


  test("testFlattenResults") {
    doTestFlattenResults()
  }

  test("testSalience") {
    doTestSalience()
  }

  test("testDebug") {
    doTestDebug()
  }

  test("simple engine should work with connect") {
    // engine doesn't have the issue folder does
    val s = sparkSession
    import s.implicits._

    val data = Seq(
      Tuple2("c", 1),
      Tuple2("c", 1),
      Tuple2("c", 1),
      Tuple2("c", 1),
      Tuple2("c", 1),
      Tuple2("c", 1),
      Tuple2("c", 1)
    ).toDF("c", "d")

    val r = data.withColumn("*", ruleEngineRunner( rulesRaw(Seq(
      (ExpressionRule("true"),
        RunOnPassProcessor(1000, Id(1041, 1),OutputExpression(s"named_struct('c', if(d = 2, 'a', 'b'), 'd', d)"))),
      (ExpressionRule("true"),
        RunOnPassProcessor(1000, Id(1041, 1),OutputExpression(s"named_struct('c', if(d = 2, 'a', 'b'), 'd', d)"))),
    ))))
    r.collect()

  }
}
