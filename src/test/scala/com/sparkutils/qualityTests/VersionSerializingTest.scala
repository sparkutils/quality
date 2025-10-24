package com.sparkutils.qualityTests

import com.sparkutils.quality._
import com.sparkutils.quality.impl.HasRuleText
import impl.imports.RuleResultsImports.packId
import impl.util.OutputExpressionRow
import org.apache.spark.sql.functions._
import org.junit.Test
import org.scalatest.FunSuite
import simpleVersioning._

class VersionSerializingTest extends FunSuite with TestUtils {

  /**
   * Verify versions with all combos.
   *
   * Lambdas are additive but will select the highest version (global or otherwise).
   * Output expressions are exact.
   */
  @Test
  def ruleEngineSuiteVersionedRoundTripsDF: Unit = evalCodeGens {
    // literal description of what should be in the files - not the objects
    val (rsIdA1, rsIdA2, rsIdA3) = (Id(1,1), Id(1,2), Id(1,3))
    val rulesA1 = RuleSuite(rsIdA1, Seq(
      RuleSet(Id(50, 1), Seq(
        Rule(Id(101, 1), ExpressionRule("a"), RunOnPassProcessor(10, Id(101,1), OutputExpression("a"))),
        Rule(Id(102, 1), ExpressionRule("b"), RunOnPassProcessor(20, Id(102,1), OutputExpression("b"))),
        Rule(Id(103, 1), ExpressionRule("c"), RunOnPassProcessor(30, Id(103,1), OutputExpression("c"))),
        Rule(Id(104, 1), ExpressionRule("d"), RunOnPassProcessor(40, Id(104,1), OutputExpression("d")))
      )),
      RuleSet(Id(51, 1), Seq(
        Rule(Id(105, 1), ExpressionRule("e"), RunOnPassProcessor(50, Id(105,1), OutputExpression("e"))),
        Rule(Id(106, 1), ExpressionRule("f"), RunOnPassProcessor(60, Id(106,1), OutputExpression("f"))),
        Rule(Id(107, 1), ExpressionRule("g"), RunOnPassProcessor(70, Id(107,1), OutputExpression("g"))),
        Rule(Id(108, 1), ExpressionRule("h"), RunOnPassProcessor(80, Id(108,1), OutputExpression("h")))
      )),
      RuleSet(Id(52, 1), Seq(
        Rule(Id(109, 1),ExpressionRule("i"), RunOnPassProcessor(90, Id(109, 1), OutputExpression("i"))),
        Rule(Id(110, 1), ExpressionRule("j"), RunOnPassProcessor(100, Id(110, 1), OutputExpression("j"))),
        Rule(Id(111, 1), ExpressionRule("k"), RunOnPassProcessor(110, Id(111, 1), OutputExpression("k"))),
        Rule(Id(112, 1), ExpressionRule("l"), RunOnPassProcessor(120, Id(112, 1), OutputExpression("l")))
      ))
    ), Seq(
      LambdaFunction("func1", "expr1", Id(200,1)),
      LambdaFunction("func2", "expr2", Id(201,1))
    ))

    val add1 = Rule(Id(103, 2), ExpressionRule("2c"), RunOnPassProcessor(30, Id(103,2), OutputExpression("2c")))
    val add2 = Rule(Id(108, 2), ExpressionRule("2h"), RunOnPassProcessor(80, Id(108,2), OutputExpression("2h")))
    val add3 = RuleSet(Id(54, 1), Seq(
      Rule(Id(150, 1), ExpressionRule("150z"), RunOnPassProcessor(80, Id(108,2), OutputExpression("2h")))
    ))

    // add two new rules and two new expressions (would be paired)
    val rulesA2 = RuleSuite(rsIdA2, Seq(
      RuleSet(Id(50, 2), Seq(
        add1
        )),
      RuleSet(Id(51, 2), Seq(
        add2
      )),
      add3
    ), Seq())
    // remove an expression and replace a lambda
    val rulesA3 = RuleSuite(rsIdA3, Seq(
      RuleSet(Id(53, 2), Seq(
        Rule(Id(112, 2), ExpressionRule("DELETED"), RunOnPassProcessor(120, Id(112,1), OutputExpression("l")))
      ))
    ), Seq(
      LambdaFunction("func1", "expr1", Id(200,2))
    ))

    // these are the actual rulesuites
    val expectedA2 = rulesA2.copy(ruleSets = rulesA1.ruleSets.map(set =>
      set.id.id match {
        case 50 => set.copy(rules = set.rules.filterNot(_.id.id == add1.id.id))
        case 51 => set.copy(rules = set.rules.filterNot(_.id.id == add2.id.id))
        case _ => set
      }
      ) ++ rulesA2.ruleSets, lambdaFunctions = rulesA1.lambdaFunctions )
    val expectedA3 = expectedA2.copy(ruleSets = expectedA2.ruleSets.map(set =>
        set.id.id match {
          case 53 => set.copy(rules = set.rules.filterNot(_.id.id == 112))
          case _ => set
        }
      ),
      lambdaFunctions = expectedA2.lambdaFunctions.filterNot(_.id.id == 200) ++
        rulesA3.lambdaFunctions, id = rulesA3.id)

    def getOuputExpressions(ruleSuite: RuleSuite) = {
      val flattened =
        ruleSuite.ruleSets.flatMap(rs => rs.rules.map { rule =>
          val oe = rule.runOnPassProcessor
          if (rule.expression.asInstanceOf[HasRuleText].rule != "DELETED")
            OutputExpressionRow(oe.rule, oe.id.id, oe.id.version, ruleSuite.id.id, ruleSuite.id.version)
          else
            null // just to keep the outputexpressions clean
        }.filterNot(_ eq null))
      val outputExpressionsDF = {
        import sparkSession.implicits._
        flattened.toDF
      }
      outputExpressionsDF
    }

    // use these above literally

    val outputExpressionsDF = getOuputExpressions(rulesA1) union
      getOuputExpressions(rulesA2) union
      getOuputExpressions(rulesA3)
    debug(outputExpressionsDF.show())

    val lambdaDF = toLambdaDS(rulesA1) union
      toLambdaDS(rulesA2) union
      toLambdaDS(rulesA3)
    debug(lambdaDF.show())

    val df = toDS(rulesA1) union
      toDS(rulesA2) union
      toDS(rulesA3)
    debug(df.show())

    val rereadWithoutLambdas = readVersionedRulesFromDF(df.toDF(),
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

    val lambdas = readVersionedLambdasFromDF(lambdaDF.toDF(),
      col("name"),
      col("ruleExpr"),
      col("functionId"),
      col("functionVersion"),
      col("ruleSuiteId"),
      col("ruleSuiteVersion")
    )

    val outputExpressions = readVersionedOutputExpressionsFromDF(outputExpressionsDF.toDF(),
      col("ruleExpr"),
      col("functionId"),
      col("functionVersion"),
      col("ruleSuiteId"),
      col("ruleSuiteVersion")
    )

    val rereadWithLambdas = integrateVersionedLambdas(rereadWithoutLambdas, lambdas)
    val (reread, missingOutputExpressions) = integrateVersionedOutputExpressions(rereadWithLambdas, outputExpressions)

    def assertEq(id: Id, expected: RuleSuite) {
      val reRules = reread.getOrElse(id, fail("Could not read the rule back"))

      def toOrdered(ruleSuite: RuleSuite): RuleSuite = {
        RuleSuite(ruleSuite.id,
          ruleSuite.ruleSets.map(rs => RuleSet(rs.id, rs.rules.toVector.sortBy(r => packId(r.id)))).toVector.
            sortBy(rs => packId(rs.id))
        )
      }

      assert(toOrdered(expected) == toOrdered(reRules), s"The rules for $id were not identical")
    }

    assertEq(rsIdA1, rulesA1)
    assertEq(rsIdA2, expectedA2)
    assertEq(rsIdA3, expectedA3)

  }

}
