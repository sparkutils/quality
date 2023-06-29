package com.sparkutils.qualityTests

import com.sparkutils.quality._
import impl.util.OutputExpressionRow
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.junit.Test
import org.scalatest.FunSuite

class RoundTripTest extends FunSuite with RowTools with TestUtils {

  @Test
  def verifyPacking(): Unit = {
    import implicits._
    val ruleId = Id(123,50483)
    val ruleTo = versionedIdTo(ruleId)
    val ruleFrom = versionedIdTo.invert(ruleTo)
    assert(ruleId == ruleFrom, "RuleId did not round trip" )
  }

  @Test
  def ruleEvalToStructAndEncodeBack: Unit = evalCodeGens {
    val rules = genRules(27, 27)
    val df = taddDataQuality(dataFrameLong(writeRows, 27, ruleSuiteResultType, null), rules, compileEvals = false) // false for coverage public needs checking

    import implicits._

    //implicit val newenc = implicitly[Encoder[RuleSuiteResult]]
    //newenc.schema.printTreeString()
    val ds = df.select("DataQuality.*").as[RuleSuiteResult]
    // it's sufficient to run the results for this test, if it can't be encoded it will throw on count
    assert(ds.count == writeRows + 1)
  }

  @Test
  def ruleEvalToOverallAndDetailsAndEncodeBack: Unit = evalCodeGens {
    val rules = genRules(27, 27)
    val df = dataFrameLong(writeRows, 27, ruleSuiteResultType, null).
      transform(taddOverallResultsAndDetailsF(rules))

    import implicits._

    val ds = df.select("DQ_Details.*").as[RuleSuiteResultDetails]
    // it's sufficient to run the results for this test, if it can't be encoded it will throw on count
    assert(ds.count == writeRows + 1)

    val dso = df.select("DQ_overallResult")
    // it's sufficient to run the results for this test, if it can't be encoded it will throw on count
    assert(dso.count == writeRows + 1)
  }

  @Test
  def ruleEvalToStructAndEncodeBackWithUserType: Unit = evalCodeGens {
    val rules = genRules(27, 27)
    val df = taddDataQuality(dataFrameLong(writeRows, 27, ruleSuiteResultType, null), rules)

    import frameless._

    import com.sparkutils.quality.implicits._

    implicit val enc = TypedExpressionEncoder[(TestIdLeft, RuleSuiteResult)]

    //newenc.schema.printTreeString()
    val ds = df.selectExpr("named_struct('left_lower', `1`, 'left_higher', `2`)","DataQuality").as[(TestIdLeft, RuleSuiteResult)]
    ds.show
    // it's sufficient to run the results for this test, if it can't be encoded it will throw on count
    assert(ds.count == writeRows + 1)
  }

  // translate into expr's instead of the default
  ///   counter expr, expr to aggregate, evaluation expr of counter and aggregate

  /**
   * Disk writing forces compilation
   */
  @Test
  def ruleEvalAndBackViaDisk: Unit = evalCodeGens {
    registerQualityFunctions()
    val rules = genRules(27, 27)
    val df = dataFrameLong(writeRows, 27, ruleSuiteResultType, null)

    df.write.mode(SaveMode.Overwrite).parquet(outputDir+"/ruleRes")
    val rere = taddDataQuality( df.sparkSession.read.parquet(outputDir+"/ruleRes"), rules )
    // won't work with code gen as it's reducing columns
//    rere.select(explode(col("DataQuality.ruleSetResults"))).show()
    rere.select(col("*"), explode(col("DataQuality.ruleSetResults"))).show()

    // it's sufficient to run the results for this test, if it can't be encoded it will throw on count
    assert(rere.count == writeRows + 1)
  }

  /**
   * Verify roundtripping of storage
   */
  @Test
  def ruleSuiteRoundTrippingToDF: Unit = evalCodeGens {
    val rsId = Id(1,1)
    val rules = RuleSuite(rsId, Seq(
      RuleSet(Id(50, 1), Seq(
        Rule(Id(100, 1), ExpressionRule("a")),
        Rule(Id(100, 2), ExpressionRule("b")),
        Rule(Id(100, 3), ExpressionRule("c")),
        Rule(Id(100, 4), ExpressionRule("d"))
      )),
      RuleSet(Id(50, 2), Seq(
        Rule(Id(100, 5), ExpressionRule("e")),
        Rule(Id(100, 6), ExpressionRule("f")),
        Rule(Id(100, 7), ExpressionRule("g")),
        Rule(Id(100, 8), ExpressionRule("h"))
      )),
      RuleSet(Id(50, 3), Seq(
        Rule(Id(100, 9),ExpressionRule("i")),
        Rule(Id(100, 10), ExpressionRule("j")),
        Rule(Id(100, 11), ExpressionRule("k")),
        Rule(Id(100, 12), ExpressionRule("l"))
      ))
    ), Seq(
      LambdaFunction("func1", "expr1", Id(200,134)),
      LambdaFunction("func2", "expr2", Id(201,131))
    ))

    val lambdaDF = toLambdaDS(rules)
    lambdaDF.show()

    val df = toRuleSuiteDF(rules)
    val rereadWithoutLambdas = readRulesFromDF(df,
      col("ruleSuiteId"),
      col("ruleSuiteVersion"),
      col("ruleSetId"),
      col("ruleSetVersion"),
      col("ruleId"),
      col("ruleVersion"),
      col("ruleExpr")
    )

    val lambdas = readLambdasFromDF(lambdaDF.toDF(),
      col("name"),
      col("ruleExpr"),
      col("functionId"),
      col("functionVersion"),
      col("ruleSuiteId"),
      col("ruleSuiteVersion")
    )

    val reread = integrateLambdas(rereadWithoutLambdas, lambdas)

    val reRules = reread.getOrElse(rsId, fail("Could not read the rule back"))

    def toOrdered(ruleSuite: RuleSuite): RuleSuite = {
      RuleSuite(ruleSuite.id,
        ruleSuite.ruleSets.map( rs => RuleSet(rs.id, rs.rules.sortBy(r => packId(r.id)))).
          sortBy(rs => packId(rs.id))
      )
    }

    assert(toOrdered(rules) == toOrdered(reRules), "The rules were not identical")
  }

  /**
   * Verify roundtripping of storage
   */
  @Test
  def ruleEngineSuiteRoundTrippingToDF: Unit = evalCodeGens {
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

    // treat these as globals
    val global = Id(-1,-1)
    val flattened =
      rules.ruleSets.flatMap(rs => rs.rules.map { rule =>
        val oe = rule.runOnPassProcessor
        OutputExpressionRow(oe.rule, oe.id.id, oe.id.version, global.id, global.version)
      })
    val outputExpressionsDF = {
      import sparkSession.implicits._
      flattened.toDF
    }
    outputExpressionsDF.show()

    val lambdaDF = toLambdaDS(rules)
    lambdaDF.show()

    val df = toDS(rules)
    val rereadWithoutLambdas = readRulesFromDF(df.toDF(),
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

    val lambdas = readLambdasFromDF(lambdaDF.toDF(),
      col("name"),
      col("ruleExpr"),
      col("functionId"),
      col("functionVersion"),
      col("ruleSuiteId"),
      col("ruleSuiteVersion")
    )

    val outputExpressions = readOutputExpressionsFromDF(outputExpressionsDF.toDF(),
      col("ruleExpr"),
      col("functionId"),
      col("functionVersion"),
      col("ruleSuiteId"),
      col("ruleSuiteVersion")
    )

    val rereadWithLambdas = integrateLambdas(rereadWithoutLambdas, lambdas)
    val (reread, missingOutputExpressions) = integrateOutputExpressions(rereadWithLambdas, outputExpressions, Some(global))

    missingOutputExpressions.size

    val reRules = reread.getOrElse(rsId, fail("Could not read the rule back"))

    def toOrdered(ruleSuite: RuleSuite): RuleSuite = {
      RuleSuite(ruleSuite.id,
        ruleSuite.ruleSets.map( rs => RuleSet(rs.id, rs.rules.toVector.sortBy(r => packId(r.id)))).toVector.
          sortBy(rs => packId(rs.id))
      )
    }

    assert(toOrdered(rules) == toOrdered(reRules), "The rules were not identical")
  }

}
