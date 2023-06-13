package com.sparkutils.qualityTests

import com.sparkutils.quality._
import com.sparkutils.quality.impl.RuleEngineRunner
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, DataType, IntegerType, StructField, StructType}
import org.junit.Test
import org.scalatest.FunSuite
import java.io.{ByteArrayOutputStream, ObjectOutputStream}

case class TestOn(product: String, account: String, subcode: Int)

case class NewPosting(transfer_type: String, account: String, product: String, subcode: Int)
case class Posting(transfer_type: String, account: String)

class RuleEngineTest extends FunSuite with TestUtils {

  val testData=Seq(
    TestOn("edt", "4201", 40),
    TestOn("otc", "5201", 40),
    TestOn("fi", "4251", 50),
    TestOn("fx", "4206", 90),
    TestOn("fxotc", "4201", 40),
    TestOn("eqotc", "4201", 60)
  )

  val DDL = "ARRAY<STRUCT<`transfer_type`: STRING, `account`: STRING, `product`: STRING, `subcode`: INTEGER >>"

  def debugRules(expressionRules: (ExpressionRule, RunOnPassProcessor) *) =
    irules(expressionRules, true)

  def rules(expressionRules: (ExpressionRule, RunOnPassProcessor) *) =
    irules(expressionRules)

  def irules(expressionRules: Seq[(ExpressionRule, RunOnPassProcessor)], debugMode: Boolean = false, compileEvals: Boolean = true, transformRuleSuite: RuleSuite => RuleSuite = identity) = {
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

    (dataFrame: DataFrame) =>
      ruleEngineRunner(transformRuleSuite(ruleSuite), DataType.fromDDL(DDL), debugMode = debugMode,
        resolveWith = if (doResolve.get()) Some(dataFrame) else None, compileEvals = compileEvals)
  }

  @Test
  def testSimpleProductionRules(): Unit = evalCodeGens {
    val rer = irules(
      Seq((ExpressionRule("product = 'edt' and subcode = 40"), RunOnPassProcessor(1000, Id(1040,1),
        OutputExpression("array(account_row('from'), account_row('to', 'other_account1'))"))),
        (ExpressionRule("product like '%fx%'"), RunOnPassProcessor(1000, Id(1042,1),
          OutputExpression("array(named_struct('transfer_type', 'from', 'account', 'another_account', 'product', product, 'subcode', subcode), named_struct('transfer_type', 'to', 'account', account, 'product', product, 'subcode', subcode))"))),
        (ExpressionRule("product = 'eqotc'"), RunOnPassProcessor(1000, Id(1043,1),
          OutputExpression("array(subcode('fromWithField', 6000), account_row('to', 'other_account1'))")))
      ), compileEvals = false
    )

    val testDataDF = {
      import sparkSession.implicits._
      testData.toDF()
    }

    import com.sparkutils.quality.implicits._

    val outdf = testDataDF.withColumn("together", rer(testDataDF))
    //outdf.show
    outdf.select("together.*").show
    val res = outdf.select("together.*").as[RuleEngineResult[Seq[NewPosting]]].collect()

    // this row will fail as the 0.6 doesn't class as a pass for the output expression - regardless of overall status
    assert(res(0).result.contains( Seq(NewPosting("from", "4201", "edt", 40), NewPosting("to","other_account1", "edt", 40))) )
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

  @Test
  def testProbabilityRuleFail = doTestProbabilityRules(OverallResult(currentResult = Failed))

  @Test
  def testProbabilityRulePass = doTestProbabilityRules(OverallResult(probablePass = 0.6, currentResult = Passed))

  def doTestProbabilityRules(overallResult: OverallResult): Unit = evalCodeGens {
    val rer = irules(
      Seq((ExpressionRule("0.6"), RunOnPassProcessor(1000, Id(1040,1),
        OutputExpression("array(account_row('from'), account_row('to', 'other_account1'))"))))
      , transformRuleSuite = _.withProbablePass(overallResult.probablePass))

    val testDataDF = {
      import sparkSession.implicits._
      testData.toDF()
    }

    import com.sparkutils.quality.implicits._

    val outdf = testDataDF.withColumn("together", rer(testDataDF))

    val res = outdf.select("together.*").as[RuleEngineResult[Seq[Posting]]].collect()
    assert(res(0).result.isEmpty)
    assert(res(0).salientRule.isEmpty)
    assert(res(0).ruleSuiteResults.overallResult == overallResult.currentResult)
  }

  @Test
  //@elidable(1) // does not work on 3.2 need to elide - possibly due to https://issues.apache.org/jira/browse/SPARK-37392 and Databricks' ES-213117
  def testFlattenResults(): Unit =  forceInterpreted { // evalCodeGensNoResolve {
    val rer = rules(
      (ExpressionRule("product = 'edt' and subcode = 40"), RunOnPassProcessor(1000, Id(1040,1),
        OutputExpression("array(account_row('from', account), account_row('to', 'other_account1'))"))),
      (ExpressionRule("product like '%fx%'"), RunOnPassProcessor(1000, Id(1041,1),
        OutputExpression("array(named_struct('transfer_type', 'from', 'account', 'another_account', 'product', product, 'subcode', subcode), named_struct('transfer_type', 'to', 'account', account, 'product', product, 'subcode', subcode))")))
    )

    import sparkSession.implicits._

    val testDataDF = testData.toDF()

    //val outdf = testDataDF.withColumn("together", rer(testDataDF)).selectExpr("flattenRuleResults(together) as expl").selectExpr("explode(expl) as tostar").selectExpr("tostar.*")
    val outdfi = testDataDF.withColumn("together", rer(testDataDF)).selectExpr("explode(flattenRuleResults(together)) as expl")

    println("outdfi show")

    outdfi.show
    outdfi.printSchema

    //val outdf = outdfi.selectExpr("explode(expl) as tostar").selectExpr("tostar.*")
    /* comment out from here to "down to here" to just use directly, which doesn't require cache
    val outdf = outdfi.selectExpr("expl.*")
    outdf.show
    outdf.printSchema

    println("RESULT SCHEMA")
    //outdf.cache // uncomment this to get the tests passing on 3.2
    val interim = outdf.selectExpr("result")
    */
    // "down to here" - comment to above marker and uncomment below to also work without cache
    val interim = outdfi.selectExpr("expl.result")
    interim.printSchema

    interim.show
    val res = interim.as[Seq[Posting]].collect()
    //val res = interim.as[Seq[Row]].collect()
    //val res = interim.as[Seq[NewPosting]].collect()
    assert(res(0) == Seq(Posting("from", "4201"), Posting("to","other_account1")))
    assert(res(6) == Seq(Posting("from", "another_account"), Posting("to","4206")))
    assert(res(8) == Seq(Posting("from", "another_account"), Posting("to","4201"))) /**/
  }

  @Test
  def testSalience(): Unit = evalCodeGens {
    val rer = rules(
      (ExpressionRule("product = 'eqotc' and account = '4201'"), RunOnPassProcessor(100, Id(1040,1),
        OutputExpression("array(updateField(account_row('fr', account), 'transfer_type', 'from'), account_row('to', 'other_account1'))"))),
      (ExpressionRule("account = '4201'"), RunOnPassProcessor(1000, Id(1041,1),
        OutputExpression("array(named_struct('transfer_type', 'from', 'account', 'another_account', 'product', product, 'subcode', subcode), named_struct('transfer_type', 'to', 'account', account, 'product', product, 'subcode', subcode))")))
    )

    import sparkSession.implicits._
    val testDataDF = testData.toDF()

    val outdf = testDataDF.withColumn("together", rer(testDataDF)).selectExpr("*", "together.result")
    outdf.show

    val res = outdf.select("result").as[Seq[Posting]].collect()
    val just4201 = Seq(Posting("from", "another_account"), Posting("to","4201"))
    assert(res(0) == just4201)
    assert(res(4) == just4201)
    assert(res(5) == Seq(Posting("from", "4201"), Posting("to","other_account1")))

    // prove unpackIdTriple works
    val srule = outdf.selectExpr("unpackIdTriple(together.salientRule) as salientRule").selectExpr("salientRule.*")
    srule.show

    // need Option for the int's because they may be null.
    val sruleres = srule.select("ruleSuiteId","ruleSuiteVersion","ruleSetId","ruleSetVersion","ruleId","ruleVersion").
      as[(Option[Int],Option[Int],Option[Int],Option[Int],Option[Int],Option[Int])].collect
    assert(sruleres(0) == (Some(1),Some(1),Some(50),Some(1),Some(100),Some(1)))
    // prove it's all nulls here i.e. salientRule is null if no rule matched
    val nulls = (None,None,None,None,None,None)
    assert(sruleres(1) == nulls)
    assert(sruleres(2) == nulls)
    assert(sruleres(3) == nulls)
  }

  @Test
  def testDebug(): Unit = evalCodeGens {
    val rer = debugRules(
      (ExpressionRule("product = 'eqotc' and account = '4201'"), RunOnPassProcessor(100, Id(1040,1),
        OutputExpression("array(account_row('from', account), account_row('to', 'other_account1'))"))),
      (ExpressionRule("account = '4201'"), RunOnPassProcessor(1000, Id(1041,1),
        OutputExpression("array(named_struct('transfer_type', 'from', 'account', 'another_account', 'product', product, 'subcode', subcode), named_struct('transfer_type', 'to', 'account', account, 'product', product, 'subcode', subcode))")))
    )

    import sparkSession.implicits._

    val testDataDF = testData.toDF()

    val outdf = testDataDF.withColumn("together", rer(testDataDF)).selectExpr("*", "together.result")
    outdf.show
    outdf.printSchema

    val res = outdf.select("result").as[Seq[(Int, Seq[Posting])]].collect()
    val just4201 = Seq(Posting("from", "another_account"), Posting("to","4201"))
    val justSeq = (1000, just4201)
    assert(res(0) == Seq(justSeq))
    assert(res(4) == Seq(justSeq))
    assert(res(5) == Seq((100, Seq(Posting("from", "4201"), Posting("to","other_account1"))), justSeq))
  }

  @Test
  def testHugeAmountOfRulesSOE(): Unit = evalCodeGensNoResolve {
    val rer = irules(
      Seq.fill(4000)(ExpressionRule(1 to 50 map ((i: Int) => s"(product = 'edt' and subcode = ${40 + i})") mkString " or "),
        RunOnPassProcessor(1000, Id(3010, 1),
        OutputExpression("array(account_row('from', account), account_row('to', 'other_account1'))"))), compileEvals = false
    )(null.asInstanceOf[DataFrame]) // the df is irrelevant as we are NoResolving

    val rs = rer.expr.asInstanceOf[RuleEngineRunner].ruleSuite
    val ds = toDS(rs)

    val so = toOutputExpressionDS(rs)

    val ruleMapWithoutOE = readRulesFromDF(ds.toDF,
      col("ruleSuiteId"),
      col("ruleSuiteVersion"),
      col("ruleSetId"),
      col("ruleSetVersion"),
      col("ruleId"),
      col("ruleVersion"),
      col("ruleExpr"),
      col("ruleEngineSalience"),
      col("ruleEngineId"),
      col("ruleEngineVersion")
    )
    val outputExpressions = readOutputExpressionsFromDF(so.toDF(),
      col("ruleExpr"),
      col("functionId"),
      col("functionVersion"),
      col("ruleSuiteId"),
      col("ruleSuiteVersion")
    )

    val (ruleMap, missing) = integrateOutputExpressions(ruleMapWithoutOE, outputExpressions, Some(Id(-1,-1))) // non-existent but shouldn't throw key not found exception

    // attempt to serialise, it if works that's enough to pass as throwing an SOE is the problem
    val rerer = ruleEngineRunner(ruleMap.head._2,
      DataType.fromDDL(DDL))

    val bos = new ByteArrayOutputStream()
    val os = new ObjectOutputStream(bos)
    os.writeObject(rerer.expr)
    val bytes = bos.toByteArray()
  }


  @Test
  def scalarSubqueryAsOutputExpressionInStruct(): Unit = evalCodeGensNoResolve {
    v3_4_and_above {
      // assert that using a join to test with is fine even when nested
      import sparkSession.implicits._
      val seq = Seq(0, 1, 2, 3, 4)
      val df = seq.toDF("i") // Force GenericArrayData instead of UnsafeArrayData
      val tableName = "the_I_s_Have_It"
      df.createOrReplaceTempView(tableName)

      // this won't work directly as it's not serializable, it must be a 'top-level' field.
      def sub(comp: String = "> 2", tableSuffix: String = "") = s"struct((select max(i_s$tableSuffix.i) from $tableName i_s$tableSuffix where i_s$tableSuffix.i $comp))"

      val rs = RuleSuite(Id(1, 1), Seq(
        RuleSet(Id(50, 1), Seq(
          Rule(Id(101, 1), ExpressionRule("true"), RunOnPassProcessor(1000, Id(3010, 1),
            OutputExpression(sub("> main.i"))))
        ))
      ))
      val testDF = seq.toDF("i").as("main")
      testDF.collect()
      val resdf = testDF.transform(ruleEngineWithStructF(rs, StructType(Seq(StructField("col1",IntegerType)))))
      try {
        val res = resdf.selectExpr("ruleEngine.result.col1").as[Option[Int]].collect()
        assert(res.count(_.isEmpty) == 1)
        assert(res.flatten.forall(_ == 4))
      } catch {
        case t: Throwable =>
          throw t
      }
    }
  }

  @Test
  def scalarSubqueryAsOutputExpression(): Unit = evalCodeGensNoResolve {
    v3_4_and_above {
      // assert that using a join to test with is fine even when nested
      import sparkSession.implicits._
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
      val resdf = testDF.transform(ruleEngineWithStructF(rs, IntegerType))
      try {
        val res = resdf.selectExpr("ruleEngine.result").as[Option[Int]].collect()
        assert(res.count(_.isEmpty) == 1)
        assert(res.flatten.forall(_ == 4))
      } catch {
        case t: Throwable =>
          throw t
      }
    }
  }

  @Test
  def scalarSubqueryAsOutputExpressionViaLambdaParam(): Unit = evalCodeGensNoResolve {
    v3_4_and_above {
      // using subqueries in lambdas does not work, it can't see the outer scope when it's a lambda variable, assume it's something like bind being called after subquery

      // assert that using a join to test with is fine even when nested
      import sparkSession.implicits._
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
      def testRes(resdf: DataFrame) {
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
      testRes(testDF.transform(ruleEngineWithStructF(rs, IntegerType, alias = null)))
      testRes(testDF.transform(ruleEngineWithStructF(rs, IntegerType, alias = "")))
    }
  }

  @Test
  def scalarSubqueryAsOutputExpressionViaLambdaNonAttributeParam(): Unit = evalCodeGensNoResolve {
    v3_4_and_above {
      // assert that using a join to test with is fine even when nested
      import sparkSession.implicits._
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
      try {
        val resdf = testDF.transform(ruleEngineWithStructF(rs, IntegerType))
        fail("should not have got here")
      } catch {
        case q: QualityException if q.getMessage.indexOf("non-attribute parameters") > -1 => ()
        case t: Throwable =>
          throw t
      }
    }
  }

  @Test
  def scalarSubqueryAsOutputExpressionViaLambdaNoParam(): Unit = evalCodeGensNoResolve {
    v3_4_and_above {
      // in this scenario the lambda is just used to avoid repeating the subquery, pretty much just a join.

      // assert that using a join to test with is fine even when nested
      import sparkSession.implicits._
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
      val resdf = testDF.transform(ruleEngineWithStructF(rs, IntegerType)) // uses main default
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
  }
}
