package com.sparkutils.qualityTests

import com.sparkutils.quality._
import com.sparkutils.quality.impl.{RuleError, RuleLogicUtils}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.junit.Test
import org.scalatest.FunSuite

class ReplaceWithMissingAttributesTest extends FunSuite with TestUtils {

  val struct = StructType(Seq(
    StructField("fielda", IntegerType)
  ))

  val names = namesFromSchema(struct)

  def doExpressionReplaceWith(ruleText: String, expected: String, empty: Set[RuleError] => Boolean) : Unit = {
    val orule = Rule(Id(2,1), ExpressionRule(ruleText))
    val rs = RuleSuite(Id(0,1), Seq(RuleSet(Id(1,1), Seq(orule))))
    val (errors, _) = validate(struct, rs)

    val nrs = processIfAttributeMissing(rs, struct)

    val rule = nrs.ruleSets.head.rules.head
    val wrapped = impl.ExpressionRuleExpr(ruleText, impl.RuleLogicUtils.expr(expected))
    assert(rule == orule.copy(expression = wrapped))
    assert(empty(errors))
  }

  @Test
  def testNoReplace: Unit = {
    val ruleText = "fieldb > 1"
    doExpressionReplaceWith(ruleText, ruleText, _.nonEmpty)
  }

  @Test
  def testRuleDisableCoalesce: Unit = {
    val ruleText = "fieldb > 1"
    doExpressionReplaceWith(s"coalesceIfAttributesMissingDisable($ruleText)", "-2", _.nonEmpty)
  }

  @Test
  def testRuleReplaceCoalesce: Unit = {
    val ruleText = "fieldb > 1"
    doExpressionReplaceWith(s"coalesceIfAttributesMissing($ruleText, 42)", "42", _.nonEmpty)
  }

  @Test
  def testRuleNoReplaceCoalesce: Unit = {
    val ruleText = "fielda > 1"
    doExpressionReplaceWith(s"coalesceIfAttributesMissing($ruleText, 42)", ruleText, _.isEmpty)
  }

  def doTestCalledLambdaReplace(ruleText: String, test: String): Unit = {
    val orule = Rule(Id(2,1), ExpressionRule(ruleText))
    val rs = RuleSuite(Id(0,1), Seq(RuleSet(Id(1,1), Seq(orule))),
      Seq(LambdaFunction("theLambda", "variable -> fieldb > 1", Id(1,1))
      ))
    val (errors, _) = validate(struct, rs)

    val nrs = processIfAttributeMissing(rs, struct)

    val rule = nrs.ruleSets.head.rules.head
    val wrapped = impl.ExpressionRuleExpr(ruleText, impl.RuleLogicUtils.expr(test))
    assert(rule == orule.copy(expression = wrapped))
    assert(errors.nonEmpty)
  }

  // @Test NOTE this cannot pass without traversing all coalesce functions in validation step to dive in lambda lookups,
  // that's problematic for a number of reasons as we'd need to have validate output cleansed lambdas
  // TODO - if we scrap the ifAttributeMissing variant processIfAttributeMissing can go before validate, which makes more sense.  needs discussion
  def testCalledLambdaReplaceCoalesce: Unit = funNRewrites {
    doTestCalledLambdaReplace(s"coalesceIfAttributesMissing(theLambda(fielda), 42)", "42")
  }

  @Test
  def testCalledWithLambdaReplaceCoalesce: Unit = funNRewrites {
    doTestCalledWithLambdaReplace("variable -> coalesceIfAttributesMissing(fieldb > 1, true)", "variable -> true", _.nonEmpty)
  }

  def doTestCalledWithLambdaReplace(lambdaText: String, expected: String, empty: Set[RuleError] => Boolean): Unit = {
    val orule = Rule(Id(2,1), ExpressionRule(s"theLambda(fielda)"))
    val rs = RuleSuite(Id(0,1), Seq(RuleSet(Id(1,1), Seq(orule))),
      Seq(LambdaFunction("theLambda", lambdaText, Id(1,1))
      ))
    val (errors, _) = validate(struct, rs)

    val nrs = processIfAttributeMissing(rs, struct)

    val rule = nrs.lambdaFunctions.head.expr
    val func = RuleLogicUtils.expr(expected)
    assert(func == rule)
    assert(empty(errors))
  }

  @Test
  def testCalledWithLambdaNoReplaceCoalesce: Unit = funNRewrites {
    doTestCalledWithLambdaReplace("variable -> coalesceIfAttributesMissing(fielda > 1, true)", "variable -> fielda > 1", _.isEmpty)
  }

  @Test
  def testRuleReplaceWithOutputCoalesce: Unit = {
    val ruleText = "fieldb > 1"
    val oruleText = "fielda"
    doTestWithOutputReplace(s"coalesceIfAttributesMissing($ruleText, 42)", "42", oruleText, oruleText, _.nonEmpty)
  }

  @Test
  def testRuleReplaceWithOutputReplaceCoalesce: Unit = {
    val ruleText = "fieldb > 1"
    doTestWithOutputReplace(s"coalesceIfAttributesMissing($ruleText, 42)", "42", s"coalesceIfAttributesMissingDisable($ruleText)", "-2", _.nonEmpty)
  }

  @Test
  def testRuleReplaceWithOutputNoReplaceCoalesce: Unit = {
    val ruleText = "fieldb > 1"
    val oruleText = "fielda > 1"
    doTestWithOutputReplace(s"coalesceIfAttributesMissing($ruleText, 42)", "42", s"coalesceIfAttributesMissingDisable($oruleText)", oruleText, _.nonEmpty)
  }

  def doTestWithOutputReplace(expressionText: String, expectedExpressionText: String,
                              outputExpressionText: String, expectedOutputExpressionText: String, empty: Set[RuleError] => Boolean): Unit = {
    val orule = Rule(Id(2,1), ExpressionRule(expressionText),
      RunOnPassProcessor(100, Id(3,1), OutputExpression(outputExpressionText)))
    val rs = RuleSuite(Id(0,1), Seq(RuleSet(Id(1,1), Seq(orule))))
    val (errors, _) = validate(struct, rs)

    val nrs = processIfAttributeMissing(rs, struct)

    val rule = nrs.ruleSets.head.rules.head
    // note although we expect a corrected expression the text looks like the original
    val wrapped = impl.ExpressionRuleExpr(expressionText, RuleLogicUtils.expr(expectedExpressionText))
    val owrapped = impl.OutputExpressionExpr(outputExpressionText, RuleLogicUtils.expr(expectedOutputExpressionText))
    assert(rule == orule.copy(expression = wrapped, runOnPassProcessor = orule.runOnPassProcessor.withExpr(owrapped)))
    assert(empty(errors))
  }

  @Test
  def testWithOutputReplaceCoalesce: Unit = {
    val ruleText = "fielda > 1"
    doTestWithOutputReplace(ruleText, ruleText,"coalesceIfAttributesMissingDisable(fieldb > 1)", "-2", _.nonEmpty)
  }

  @Test
  def testWithOutputNoReplaceCoalesce: Unit = {
    val ruleText = "fielda > 1"
    doTestWithOutputReplace(ruleText, ruleText,s"coalesceIfAttributesMissingDisable($ruleText)", ruleText, _.isEmpty)
  }

  @Test
  def testCoalesceNested: Unit = {
    val exprText = "isNull(coalesceIfAttributesMissing( fieldb > 0, coalesceIfAttributesMissing(fieldb < 3, coalesceIfAttributesMissing( twentyfieldsDeep,  fielda <> 19 ) )))"

    val expr = processCoalesceIfAttributeMissing( RuleLogicUtils.expr(exprText), names )
    val expected = RuleLogicUtils.expr("isNull(fielda <> 19)")
    assert(expr == expected)
  }

  @Test
  def testCoalesceNestedNull: Unit = {
    val exprText = "isNull(coalesceIfAttributesMissing( fieldb > 0, coalesceIfAttributesMissing(fieldb < 3, coalesceIfAttributesMissing( twentyfieldsDeep,  fieldb <> 19 ) )))"

    val expr = processCoalesceIfAttributeMissing( RuleLogicUtils.expr(exprText), names )
    val expected = RuleLogicUtils.expr("isNull(null)")
    assert(expr == expected)
  }

}
