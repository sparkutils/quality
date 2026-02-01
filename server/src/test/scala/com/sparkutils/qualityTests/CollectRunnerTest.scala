package com.sparkutils.qualityTests

import com.sparkutils.quality.{DefaultProcessor, DefaultRule, ExpressionRule, Failed, Id, LambdaFunction, OutputExpression, Passed, Rule, RuleFolderResult, RuleResult, RuleSet, RuleSuite, RunOnPassProcessor, collectRunner, registerLambdaFunctions}
import com.sparkutils.qualityTests.util.SharedPureConnectTests
import frameless.TypedEncoder
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, StringType, StructField, StructType}
import org.scalatest.Matchers.convertToAnyShouldWrapper

import scala.reflect.ClassTag

trait CollectRunnerTestBase extends SharedPureConnectTests {

  val testData=Seq(
    TestOn("edt", "4201", 40),
    TestOn("otc", "5201", 40),
    TestOn("fi", "4251", 50),
    TestOn("fx", "4206", 90),
    TestOn("fxotc", "4201", 40),
    TestOn("eqotc", "4201", 60)
  )

  val inPlace = new scala.util.DynamicVariable[Boolean](true)
  val inPlaceUnroll = new scala.util.DynamicVariable[Boolean](false)
  val inPlaceUnrollSize = new scala.util.DynamicVariable[Int](1)

  def irules(expressionRules: Seq[(ExpressionRule, RunOnPassProcessor)])(
    debugMode: Boolean = false, // it will likely never be added
             transformRuleSuite: RuleSuite => RuleSuite = identity,
    flatten: Boolean = true, includeNulls: Boolean = false,
    dataType: Option[DataType] = Some(
      ArrayType(StructType(Seq(
        StructField("transfer_type", StringType),
        StructField("account", StringType),
        StructField("product", StringType),
        StructField("subcode", IntegerType)
      )))
    )
  ) = {
    registerLambdaFunctions(Seq(
      LambdaFunction("account_row", "(transfer_type, account) -> named_struct('transfer_type', transfer_type, 'account', account, 'product', product, 'subcode', subcode)", Id(123, 23)),
      LambdaFunction("account_row", "transfer_type -> account_row(transfer_type, account)", Id(123, 24)),
      LambdaFunction("subcodeF", "(transfer_type, sub) -> account_row(transfer_type, string(sub))", Id(123, 25))
    ))

    val rules =
      for { ((exp, processor), idOffset) <- expressionRules.zipWithIndex }
        yield Rule(Id(100 * idOffset, 1), exp, processor)

    val rsId = Id(1, 1)
    val ruleSuite = RuleSuite(rsId, Seq(
      RuleSet(Id(50, 1), rules
      )))

    (dataFrame: DataFrame) =>
      collectRunner(transformRuleSuite(ruleSuite),
        dataType,
        flatten = flatten, includeNulls = includeNulls,
        useInPlaceArray = inPlace.value, unrollInPlaceArray = inPlaceUnroll.value,
        unrollOutputArraySize = inPlaceUnrollSize.value)
  }

  def testBase[T: TypedEncoder: ClassTag, O: Ordering](
      expected: Seq[T], ordF: T => O, sparkTo: DataFrame => Seq[T])( debugMode: Boolean = false,
      transformRuleSuite: RuleSuite => RuleSuite = identity, flatten: Boolean = true,
      includeNulls: Boolean = false, nullInArray: Boolean = false,
      dummyOut: String = "array(account_row('whoknows', 'money'))", canRunSimpleSpark: Boolean = true,
      testData: Seq[TestOn] = testData, result: RuleResult = Passed
  ): Unit = {
    testBaseI[T, O](expected, ordF, sparkTo)( debugMode = debugMode,
      transformRuleSuite = transformRuleSuite, flatten = flatten,
      includeNulls = includeNulls, nullInArray = nullInArray,
      dummyOut = dummyOut, canRunSimpleSpark = canRunSimpleSpark,
      testData = testData, result = result)
    // derive type case
    testBaseI[T, O](expected, ordF, sparkTo)( debugMode = debugMode,
      transformRuleSuite = transformRuleSuite, flatten = flatten,
      includeNulls = includeNulls, nullInArray = nullInArray,
      dummyOut = dummyOut, canRunSimpleSpark = canRunSimpleSpark, dataType = None,
      testData = testData, result = result)
  }

// TODO add primitive tests for Databricks janino version problem
  def thunker(thunk: => Unit): Unit = {
    thunk

    inPlace.withValue(false){
      thunk
    }

    inPlaceUnroll.withValue(true){
      thunk // as for loop
      inPlaceUnrollSize.withValue(2) {
        thunk // unroll in a loop
      }
      inPlaceUnrollSize.withValue(6) {
        thunk // full unroll
      }
    } // TODO add a test case for a large number of entries to force a loop and overflow

  }

  def testBaseI[T: TypedEncoder: ClassTag, O: Ordering](
      expected: Seq[T], ordF: T => O, sparkTo: DataFrame => Seq[T])( debugMode: Boolean = false,
      transformRuleSuite: RuleSuite => RuleSuite = identity, flatten: Boolean = true,
      includeNulls: Boolean = false, nullInArray: Boolean = false,
      dummyOut: String = "array(account_row('whoknows', 'money'))", canRunSimpleSpark: Boolean = true,
      dataType: Option[DataType] = Some(
        ArrayType(StructType(Seq(
          StructField("transfer_type", StringType),
          StructField("account", StringType),
          StructField("product", StringType),
          StructField("subcode", IntegerType)
        )))
      ), testData: Seq[TestOn] = testData, result: RuleResult = Passed
    ): Unit = thunker {
    val rer = irules(
      Seq(
        (ExpressionRule("product = 'eqotc'"), RunOnPassProcessor(1001, Id(1044,1),
          OutputExpression(dummyOut))),
        (ExpressionRule("product = 'fred'"), RunOnPassProcessor(1001, Id(1044,1),
          OutputExpression(dummyOut))),

        (ExpressionRule("product = 'edt' and subcode = 40"), RunOnPassProcessor(995, Id(1040,1),
          OutputExpression("array(subcodeF('from', 1234), account_row('to'))"))),
        (ExpressionRule("product like '%fx%'"), RunOnPassProcessor(996, Id(1042,1),
          OutputExpression("array(account_row('to'), account_row('from'))"))),
        (ExpressionRule("product = 'eqotc'"), RunOnPassProcessor(1000, Id(1043,1),
          OutputExpression(s"array(account_row('from'), ${if (nullInArray) "null" else "account_row('to')"})")))

      )
    )(debugMode = debugMode, transformRuleSuite = transformRuleSuite, flatten = flatten,
      includeNulls = includeNulls, dataType = dataType)

    val testDataDF = {
      val s = sparkSession
      import s.implicits._

      // force code gen, LocalRelation doesn't.
      testData.toDF().repartition(4).write.mode(SaveMode.Overwrite).parquet(outputDir + "/collectRunner")
      sparkSession.read.parquet(outputDir + "/collectRunner")
    }

    import com.sparkutils.quality.implicits._

    val outdf = testDataDF.withColumn("together", rer(testDataDF))

    lazy val got = sparkTo(outdf.select("together.*").select(explode(col("result")).as("exp")))

    def verify(got: Seq[T], expected: Seq[T]): Unit = {
      val sortedGot = got.sortBy( ordF )
      val sortedExp = expected.sortBy( ordF ).toVector
      sortedGot shouldBe sortedExp
    }

    // encoder can't handle nulls in some versions, Frameless handles them all
    if (canRunSimpleSpark) {
      verify(got, expected)
    }

    val viaFrameless = outdf.select("together.*").as[RuleFolderResult[Seq[T]]].collect()

    if (result != Passed) {
      // default or failed
      viaFrameless.map(_.ruleSuiteResults.overallResult == result).forall(identity) shouldBe true
    }
    verify(viaFrameless.flatMap(_.result).flatten[T], expected)
  }

  test("simpleProductionRules") { thunker {
    import com.sparkutils.quality.implicits._

    val s = sparkSession
    import s.implicits._

    testBase(Seq(
      NewPosting("from","1234","edt", 40),
      NewPosting("whoknows","money","eqotc", 60), // our extra eqotc case
      NewPosting("to","4206","fx", 90),
      NewPosting("from","4206","fx", 90),
      NewPosting("to","4201","edt", 40),
      NewPosting("from","4201","eqotc", 60),
      NewPosting("to","4201","eqotc", 60),
      NewPosting("to","4201","fxotc", 40),
      NewPosting("from","4201","fxotc", 40)
    ), NewPosting.unapply, _.select("exp.*").as[NewPosting].collect())()

  } }

  test("no matches and with default should be default_rule flatten") {
    import com.sparkutils.quality.implicits._

    val s = sparkSession
    import s.implicits._

    testBase(Seq(
      NewPosting("started", "b", "a", 0),
      NewPosting("ended", "b", "a", 0),
      NewPosting("started", "f", "e", 1),
      NewPosting("ended", "f", "e", 1)
    ), NewPosting.unapply, _.select("exp.*").as[NewPosting].collect())(
      result = DefaultRule, testData = Seq(TestOn("a", "b", 0), TestOn("e", "f", 1)),
      transformRuleSuite = r => r.copy(defaultProcessor = DefaultProcessor(Id(123,123),
        OutputExpression("array(account_row('ended'), account_row('started'))")))
    )

  }

  test("no matches and no default should be failed") {
    import com.sparkutils.quality.implicits._

    val s = sparkSession
    import s.implicits._

    testBase(Seq(
    ), NewPosting.unapply, _.select("exp.*").as[NewPosting].collect())(
      result = Failed, testData = Seq(TestOn("a", "b", 0), TestOn("e", "f", 1))
    )

  }

  test("no matches and with default should be default_rule flatten nulls included") {
    import com.sparkutils.quality.implicits._

    val s = sparkSession
    import s.implicits._

    testBase[Option[NewPosting], Int](Seq(
      NewPosting("started", "b", "a", 0),
      null,
      NewPosting("started", "f", "e", 1),
      NewPosting("ended", "f", "e", 1)
    ).map(Option(_)), _.hashCode(), _.as[Option[NewPosting]].collect())(
      result = DefaultRule, testData = Seq(TestOn("a", "b", 0), TestOn("e", "f", 1)),
      transformRuleSuite = r => r.copy(defaultProcessor = DefaultProcessor(Id(123,123),
        OutputExpression("array(account_row('started'), if(product = 'a',  null, account_row('ended')))"))),
        includeNulls = true
    )

  }

  test("no matches and with default should be default_rule no flatten") {
    import com.sparkutils.quality.implicits._

    val s = sparkSession
    import s.implicits._

    testBase[Seq[NewPosting], Int](Seq(
      List(NewPosting("started", "b", "a", 0), NewPosting("ended", "b", "a", 0)),
      List(NewPosting("started", "f", "e", 1), NewPosting("ended", "f", "e", 1))
    ),  _.toVector.hashCode(), _.as[Seq[NewPosting]].collect())(
      result = DefaultRule, testData = Seq(TestOn("a", "b", 0), TestOn("e", "f", 1)),
      transformRuleSuite = r => r.copy(defaultProcessor = DefaultProcessor(Id(123,123),
        OutputExpression("array(account_row('started'), account_row('ended'))"))), flatten = false
    )

  }

  test("nonFlatten") { thunker {
    import com.sparkutils.quality.implicits._

    val s = sparkSession
    import s.implicits._

    testBase[Seq[NewPosting], Int](Seq(
      List(NewPosting("from","1234","edt",40), NewPosting("to","4201","edt",40)),
      List(NewPosting("to","4206","fx",90), NewPosting("from","4206","fx",90)),
      List(NewPosting("whoknows","money","eqotc",60)),
      List(NewPosting("to","4201","fxotc",40), NewPosting("from","4201","fxotc",40)),
      List(NewPosting("from","4201","eqotc",60), NewPosting("to","4201","eqotc",60))
    ), _.toVector.hashCode(), _.as[Seq[NewPosting]].collect())(flatten = false)

  } }

  // AgnosticEncoders don't allow a null list item, so they need to be Option Seq
  test("nonFlattenWithNulls") {  {
    thunker {
      import com.sparkutils.quality.implicits._

      val s = sparkSession
      import s.implicits._

      testBase[Option[Seq[NewPosting]], Int](Seq(
        List(NewPosting("from","1234","edt",40), NewPosting("to","4201","edt",40)),
        List(NewPosting("to","4206","fx",90), NewPosting("from","4206","fx",90)),
        null,
        List(NewPosting("to","4201","fxotc",40), NewPosting("from","4201","fxotc",40)),
        List(NewPosting("from","4201","eqotc",60), NewPosting("to","4201","eqotc",60))
      ).map(Option(_)), _.hashCode(),
        _.as[Option[Seq[NewPosting]]].collect())(flatten = false, includeNulls = true,
        dummyOut = "null", canRunSimpleSpark = false)

      }
    }
  }

  test("flattenWithNulls") { thunker {
    import com.sparkutils.quality.implicits._

    val s = sparkSession
    import s.implicits._

    testBase[Option[NewPosting], Int](Seq(
        NewPosting("from","1234","edt", 40),
        null, // our extra eqotc case
        NewPosting("to","4206","fx", 90),
        NewPosting("from","4206","fx", 90),
        NewPosting("to","4201","edt", 40),
        NewPosting("from","4201","eqotc", 60),
        NewPosting("to","4201","eqotc", 60),
        NewPosting("to","4201","fxotc", 40),
        NewPosting("from","4201","fxotc", 40)
      ).map(Option(_)), _.hashCode(), _.select("exp.*").as[Option[NewPosting]].collect())(includeNulls = true,
      dummyOut = "null", canRunSimpleSpark = false)

  } }

  test("flattenWithNestedNulls") { thunker {
    import com.sparkutils.quality.implicits._

    val s = sparkSession
    import s.implicits._

    testBase[Option[NewPosting], Option[(String, String, String, Int)]](Seq(
      NewPosting("from","1234","edt", 40),
      NewPosting("whoknows","money","eqotc", 60), // our extra eqotc case
      null, // our extra null in array
      NewPosting("to","4206","fx", 90),
      NewPosting("from","4206","fx", 90),
      NewPosting("to","4201","edt", 40),
      NewPosting("from","4201","eqotc", 60),
      NewPosting("to","4201","fxotc", 40),
      NewPosting("from","4201","fxotc", 40)
    ).map(Option(_)), _.flatMap(NewPosting.unapply), _.select("exp.*").as[Option[NewPosting]].collect())(includeNulls = true,
      nullInArray = true, canRunSimpleSpark = false)

  } }

  test("flattenWithTopAndNestedNulls") { thunker {
    import com.sparkutils.quality.implicits._

    val s = sparkSession
    import s.implicits._

    testBase[Option[NewPosting], Option[(String, String, String, Int)]](Seq(
      NewPosting("from","1234","edt", 40),
      null, // our extra eqotc case
      null, // our extra null in array
      NewPosting("to","4206","fx", 90),
      NewPosting("from","4206","fx", 90),
      NewPosting("to","4201","edt", 40),
      NewPosting("from","4201","eqotc", 60),
      NewPosting("to","4201","fxotc", 40),
      NewPosting("from","4201","fxotc", 40)
    ).map(Option(_)), _.flatMap(NewPosting.unapply), _.select("exp.*").as[Option[NewPosting]].collect())(includeNulls = true,
      dummyOut = "null", nullInArray = true, canRunSimpleSpark = false)

  } }

}

class CollectRunnerTest extends CollectRunnerTestBase {

}

