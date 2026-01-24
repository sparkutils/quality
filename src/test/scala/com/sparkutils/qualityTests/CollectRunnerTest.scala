package com.sparkutils.qualityTests

import com.sparkutils.quality.impl.RunOnPassProcessor
import com.sparkutils.quality.{ExpressionRule, Id, LambdaFunction, OutputExpression, Rule, RuleFolderResult, RuleSet, RuleSuite, RunOnPassProcessor, collectRunner, registerLambdaFunctions, ruleFolderRunner}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.{col, explode, lit, struct}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
import org.junit.Test
import org.scalatest.FunSuite
import org.scalatest.Matchers.convertToAnyShouldWrapper

class CollectRunnerTest  extends FunSuite with TestUtils {

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

  def irules(expressionRules: Seq[(ExpressionRule, RunOnPassProcessor)], debugMode: Boolean = false,
             transformRuleSuite: RuleSuite => RuleSuite = identity, flatten: Boolean = true, includeNulls: Boolean = false) = {
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

    import sqlContext.implicits._

    (dataFrame: DataFrame) =>
      collectRunner(transformRuleSuite(ruleSuite),
        ArrayType(StructType(Seq(
          StructField("transfer_type", StringType),
          StructField("account", StringType),
          StructField("product", StringType),
          StructField("subcode", IntegerType)
        ))),
        debugMode = debugMode, flatten = flatten, includeNulls = includeNulls)
  }

  @Test
  def testSimpleProductionRules(): Unit = evalCodeGensNoResolve { funNRewrites {
    val rer = irules(
      Seq(
        (ExpressionRule("product = 'edt' and subcode = 40"), RunOnPassProcessor(1000, Id(1040,1),
          OutputExpression("array(subcodeF('from', 1234), account_row('to'))"))),
        (ExpressionRule("product like '%fx%'"), RunOnPassProcessor(1000, Id(1042,1),
          OutputExpression("array(account_row('to'), account_row('from'))"))),
        (ExpressionRule("product = 'eqotc'"), RunOnPassProcessor(1000, Id(1043,1),
          OutputExpression("array(account_row('from'), account_row('to'))"))),
        (ExpressionRule("product = 'eqotc'"), RunOnPassProcessor(1001, Id(1044,1),
          OutputExpression("array(account_row('whoknows', 'money'))"))),
        (ExpressionRule("product = 'fred'"), RunOnPassProcessor(1001, Id(1045,1),
          OutputExpression("array(account_row('whoknows', 'money'))")))
      )
    )

    val testDataDF = {
      import sparkSession.implicits._
      // force code gen, LocalRelation doesn't.
      testData.toDF().repartition(4).write.mode(SaveMode.Overwrite).parquet(outputDir + "/collectRunner")
      sparkSession.read.parquet(outputDir + "/collectRunner")
    }

    import com.sparkutils.quality.implicits._

    val outdf = testDataDF.withColumn("together", rer(testDataDF))
    //.select(expr("*"), rer(testDataDF))
    //outdf.show
    //val results = outdf.select("together.*").selectExpr("explode(result)").select("col.*").select("result.*")
    // results.show

    import sparkSession.implicits._
    val got = outdf.select("together.*").select(explode(col("result")).as("exp")).select("exp.*").as[NewPosting]

    def verify(got: Seq[NewPosting], expected: Seq[NewPosting]): Unit = {
      val sorter = (a: NewPosting, b: NewPosting) => a.transfer_type < b.transfer_type && a.product < b.product &&
        a.account < b.account && a.subcode < b.subcode
      val sortedGot = got.sortWith( sorter )
      val sortedExp = expected.sortWith( sorter )
      sortedGot shouldBe sortedExp
    }

    val expected = Seq(
      NewPosting("from","1234","edt", 40),
        NewPosting("to","4201","edt", 40),
        NewPosting("from","4201","eqotc", 60),
        NewPosting("to","4201","eqotc", 60),
        NewPosting("whoknows","money","eqotc", 60), // our extra eqotc case
        NewPosting("to","4206","fx", 90),
        NewPosting("from","4206","fx", 90),
        NewPosting("to","4201","fxotc", 40),
        NewPosting("from","4201","fxotc", 40)
    )

    verify(got.collect(), expected)

    val viaFrameless = outdf.select("together.*").as[RuleFolderResult[Seq[NewPosting]]].collect()

    verify(viaFrameless.flatMap(_.result).flatten[NewPosting], expected)
  } }


}
