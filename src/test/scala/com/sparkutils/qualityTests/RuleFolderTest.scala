package com.sparkutils.qualityTests

import com.sparkutils.quality._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.junit.Test
import org.scalatest.FunSuite

class RuleFolderTest extends FunSuite with RowTools with TestUtils {

  val testData=Seq(
    TestOn("edt", "4201", 40),
    TestOn("otc", "5201", 40),
    TestOn("fi", "4251", 50),
    TestOn("fx", "4206", 90),
    TestOn("fxotc", "4201", 40),
    TestOn("eqotc", "4201", 60)
  )

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

    import sqlContext.implicits._

    (dataFrame: DataFrame) =>
      ruleFolderRunner(transformRuleSuite(ruleSuite), struct(lit("").as("transfer_type"), $"account", $"product", $"subcode"), debugMode = debugMode,
        resolveWith = if (doResolve.get()) Some(dataFrame) else None, compileEvals = compileEvals)
  }

  // Must use NoResolve as it fails on 2.4 with an npe, resolving on higher works fine
  @Test
  def testSimpleProductionRules(): Unit = evalCodeGensNoResolve {
    val rer = irules(
      Seq((ExpressionRule("product = 'edt' and subcode = 40"), RunOnPassProcessor(1000, Id(1040,1),
        OutputExpression("thecurrent -> updateField(updateField(thecurrent, 'subcode', 1234), 'transfer_type', 'from')"))),
        //OutputExpression("thecurrent -> updateField(thecurrent, 'subcode', 1234, 'transfer_type', 'from')")))

        (ExpressionRule("product like '%fx%'"), RunOnPassProcessor(1000, Id(1042,1),
          OutputExpression("thecurrent -> updateField(thecurrent, 'transfer_type', 'to')"))),
        (ExpressionRule("product = 'eqotc'"), RunOnPassProcessor(1000, Id(1043,1),
          OutputExpression("thecurrent -> updateField(thecurrent, 'transfer_type', 'from')"))),
        (ExpressionRule("product = 'eqotc'"), RunOnPassProcessor(1001, Id(1044,1),
          OutputExpression("thecurrent -> updateField(thecurrent, 'account', concat(account,'_fruit'))")))
      ), compileEvals = true, debugMode = true
    ) // compileEvals + codeGens IS NOT forcing a code gen on >Spark3

    val testDataDF = {
      import sparkSession.implicits._
      testData.toDF()
    }

    import com.sparkutils.quality.implicits._

    val outdf = testDataDF//.withColumn("thefield", struct(lit("").as("transfer_type"), $"account", $"product", $"subcode"))
      .withColumn("together", rer(testDataDF))
      //.select(expr("*"), rer(testDataDF))
    //outdf.show

    //val results = outdf.select("together.*").selectExpr("explode(result)").select("col.*").select("result.*")
    // results.show
    val res = outdf.select("together.*").as[RuleFolderResult[Seq[(Int,NewPosting)]]].collect()

    // this row will fail as the 0.6 doesn't class as a pass for the output expression - regardless of overall status
    assert(res(0).result.contains( Seq((1000, NewPosting("from", "4201", "edt", 1234)))) )

    //    TestOn("fxotc", "4201", 40),
    assert(res(3).result.contains(Seq((1000, NewPosting("to", "4206", "fx", 90)))))
    assert(res(4).result.contains(Seq((1000, NewPosting("to", "4201", "fxotc", 40)))))

    // did the field replace work
    assert(res(5).result.contains(Seq((1000, NewPosting("from", "4201", "eqotc", 60)), (1001, NewPosting("from", "4201_fruit", "eqotc", 60)))))

  }

  def testAndRulesForReplace(useSetSyntax: Boolean) = {
    registerLambdaFunctions(Seq(
      /*      LambdaFunction("account_row", "(transfer_type, account) -> named_struct('transfer_type', transfer_type, 'account', account, 'product', product, 'subcode', subcode)", Id(123, 23)),
            LambdaFunction("account_row", "transfer_type -> account_row(transfer_type, account)", Id(123, 24)),
            LambdaFunction("subcode", "(transfer_type, sub) -> updateField(account_row(transfer_type, account), 'subcode', sub)", Id(123, 25))*/
    ))

    val expressionRules =
      if (!useSetSyntax)
        Seq((ExpressionRule("product = 'edt' and subcode = 40"), RunOnPassProcessor(1000, Id(1040,1),
        OutputExpression("thecurrent -> updateField(thecurrent, 'subcode', 1234)"))),

        (ExpressionRule("product = 'eqotc'"), RunOnPassProcessor(1001, Id(1044,1),
          OutputExpression("thecurrent -> updateField(thecurrent, 'account', concat(thecurrent.account,'_fruit'), 'subcode', 6000)"))),
        (ExpressionRule("product like '%fx%'"), RunOnPassProcessor(1000, Id(1042,1),
          OutputExpression("thecurrent -> updateField(thecurrent, 'account', 'to')"))),
        (ExpressionRule("product = 'eqotc'"), RunOnPassProcessor(1000, Id(1043,1),
          OutputExpression("thecurrent -> updateField(thecurrent, 'account', 'from')")))
        )
      else
        Seq((ExpressionRule("product = 'edt' and subcode = 40"), RunOnPassProcessor(1000, Id(1040,1),
          OutputExpression("set(subcode = 1234)"))),

          (ExpressionRule("product = 'eqotc'"), RunOnPassProcessor(1001, Id(1044,1),
            OutputExpression("set( account = concat(currentResult.account,'_fruit'), subcode = 6000)"))),
          (ExpressionRule("product like '%fx%'"), RunOnPassProcessor(1000, Id(1042,1),
            OutputExpression("set(account = 'to')"))),
          (ExpressionRule("product = 'eqotc'"), RunOnPassProcessor(1000, Id(1043,1),
            OutputExpression("set(account = 'from')")))
        )

    val rules =
      for { ((exp, processor), idOffset) <- expressionRules.zipWithIndex }
        yield Rule(Id(100 * idOffset, 1), exp, processor)

    val rsId = Id(1, 1)
    val ruleSuite = RuleSuite(rsId, Seq(
      RuleSet(Id(50, 1), rules
      )))

    val testDataDF = {
      import sparkSession.implicits._
      testData.toDF()
    }
    //testDataDF.show
    (testDataDF, ruleSuite)
  }

  def doReplaceTest(outdf: DataFrame): Unit = {
    import sparkSession.implicits._
    outdf.show
    val res = outdf.filter("subcode is not null").as[TestOn].collect()

    assert(res(0) == TestOn("edt", "4201", 1234))

    //    TestOn("fxotc", "4201", 40),
    assert(res(1) == TestOn("fx", "to", 90))
    assert(res(2) == TestOn("fxotc", "to", 40))

    // did the field replace work
    assert(res(3) == TestOn("eqotc", "from_fruit", 6000))

  }

  @Test
  def testSimpleProductionRulesReplace(): Unit = doTestSimpleProductionRulesReplace(false)

  @Test
  def testSimpleProductionRulesReplaceSet(): Unit = doTestSimpleProductionRulesReplace(true)

  def doTestSimpleProductionRulesReplace(useSetSyntax: Boolean): Unit = evalCodeGens {
    val (testDataDF, ruleSuite) = testAndRulesForReplace(useSetSyntax)

    val outdf = testDataDF.transform(foldAndReplaceFields(ruleSuite, Seq("account", "product", "subcode")))
    //outdf.show
    doReplaceTest(outdf)
    val fields = outdf.schema.fields.map(_.name)
    assert(fields.toSeq == Seq("product", "account", "subcode", "foldedFields"))
  }

  @Test
  def testSimpleProductionRulesReplaceOutOfOrder(): Unit = doTestSimpleProductionRulesReplaceOutOfOrder(false)
  @Test
  def testSimpleProductionRulesReplaceOutOfOrderSet(): Unit = doTestSimpleProductionRulesReplaceOutOfOrder(true)

  def doTestSimpleProductionRulesReplaceOutOfOrder(useSetSyntax: Boolean): Unit = evalCodeGens {
    val (testDataDF, ruleSuite) = testAndRulesForReplace(useSetSyntax)

    val outdf = testDataDF.transform(foldAndReplaceFields(ruleSuite, Seq("account", "product", "subcode"), maintainOrder = false))
    //outdf.show
    doReplaceTest(outdf)
    val fields = outdf.schema.fields.map(_.name)
    assert(fields.toSeq == Seq("foldedFields", "account", "product", "subcode"))
  }

  @Test
  def testSimpleProductionRulesReplaceCustomDDL(): Unit = doTestSimpleProductionRulesReplaceCustomDDL(false)

  @Test
  def testSimpleProductionRulesReplaceCustomDDLSet(): Unit = doTestSimpleProductionRulesReplaceCustomDDL(true)

  def doTestSimpleProductionRulesReplaceCustomDDL(useSetSyntax: Boolean): Unit = evalCodeGens {
    val (testDataDF, ruleSuite) = testAndRulesForReplace(useSetSyntax)

    val outdf = testDataDF.transform(foldAndReplaceFieldsWithStruct(ruleSuite, StructType(Seq(StructField("account", StringType),
      StructField("product", StringType), StructField("subcode",IntegerType))), maintainOrder = false))
    //outdf.show
    doReplaceTest(outdf)
    val fields = outdf.schema.fields.map(_.name)
    assert(fields.toSeq == Seq("foldedFields", "account", "product", "subcode"))
  }

  @Test
  def testSimpleProductionRulesReplaceDebug(): Unit = doTestSimpleProductionRulesReplaceDebug(false)

  @Test
  def testSimpleProductionRulesReplaceDebugSet(): Unit = doTestSimpleProductionRulesReplaceDebug(true)

  def doTestSimpleProductionRulesReplaceDebug(useSetSyntax: Boolean): Unit = evalCodeGens {
    val (testDataDF, ruleSuite) = testAndRulesForReplace(useSetSyntax)

    val outdf = testDataDF.transform(foldAndReplaceFields(ruleSuite, Seq("account", "product", "subcode"), debugMode = true))
    //outdf.show
    doReplaceTest(outdf)

    //val results = outdf.select("together.*").selectExpr("explode(result)").select("col.*").select("result.*")
    // results.show
    import com.sparkutils.quality.implicits._
    val res = outdf.select("foldedFields.*").as[RuleFolderResult[Seq[(Int,TestOn)]]].collect()

    // purposefully the wrong way around in the call, and the encoder is positional so we need to flip here
    // this row will fail as the 0.6 doesn't class as a pass for the output expression - regardless of overall status
    assert(res(0).result.contains( Seq((1000, TestOn("4201", "edt", 1234)))) )

    //    TestOn("fxotc", "4201", 40),
    assert(res(3).result.contains(Seq((1000, TestOn("to", "fx", 90)))))
    assert(res(4).result.contains(Seq((1000, TestOn("to", "fxotc", 40)))))

    // did the field replace work
    assert(res(5).result.contains(Seq((1000, TestOn("from", "eqotc", 60)), (1001, TestOn("from_fruit", "eqotc", 6000)))))

  }

  @Test
  def testFlattenResults(): Unit = doTestFlattenResults(false)

  @Test
  def testFlattenResultsSet(): Unit = doTestFlattenResults(true)

  def doTestFlattenResults(useSetSyntax: Boolean): Unit =  evalCodeGens {//forceInterpreted { // evalCodeGensNoResolve {
    val (testDataDF, ruleSuite) = testAndRulesForReplace(useSetSyntax)

    import sparkSession.implicits._

    testDataDF.write.mode("overwrite").parquet(outputDir+"/ruleFolder")
    val loadedDF = sparkSession.read.parquet(outputDir+"/ruleFolder")

    val outdfit = loadedDF.
      withColumn("together",
        ruleFolderRunner(ruleSuite, struct(lit("").as("transfer_type"),
          $"product", $"account", $"subcode") /*, useType = Some(
          StructType(Seq(StructField("transfer_type", StringType),
            StructField("product", StringType),
            StructField("account", StringType),
            StructField("subcode", IntegerType)
          ))
        )*/))

    val outdfi = outdfit.selectExpr("explode(flattenFolderResults(together)) as expl")

    //println("outdfi show")

    //outdfi.show
    //outdfi.printSchema

    val interim = outdfi.selectExpr("expl.result.*")
    //interim.printSchema

    //interim.show - orderBy needed as the filesystem messes order up
    val res = interim.filter("subcode is not null").orderBy("product").as[TestOn].collect()

    // 4 rules == 4 rows each

    for{ i <- 0 until 4}
      assert(res(i) == TestOn("edt", "4201", 1234))

    //    TestOn("fxotc", "4201", 40),
    for{ i <- 4 until 8}
      assert(res(i) == TestOn("eqotc", "from_fruit", 6000))

    for{ i <- 8 until 12}
      assert(res(i) == TestOn("fx", "to", 90))

    // did the field replace work
    for{ i <- 12 until 16}
      assert(res(i) == TestOn("fxotc", "to", 40))

  }

  @Test
  def testSetSyntaxButNoEqualTo(): Unit = {
    val bad = OutputExpression("set('lit')").expr
    assert(bad.children.head.getClass == Literal("lit").getClass)
  }

  @Test
  def testSetSyntaxEqualToButNoAttribute(): Unit = {
    val bad = OutputExpression("set( 1 = 'lit' )").expr
    assert(bad.children.head.children.head.getClass == Literal("lit").getClass)
  }
}
