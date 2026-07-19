package com.sparkutils.qualityTests.classicOnly

import com.sparkutils.quality.impl.ExpressionRule
import com.sparkutils.quality.impl.extension.ZeroCodeGen
import com.sparkutils.quality.{Id, Rule, RuleSet, RuleSuite, ruleRunner}
import com.sparkutils.qualityTests.util.ClassicSharedTests
import org.apache.spark.sql.ShimUtils
import org.apache.spark.sql.catalyst.expressions.{BoundReference, CaseWhen, EqualTo, EquivalentExpressions, Literal}
import org.apache.spark.sql.types.{BooleanType, IntegerType}
import org.scalatest.Matchers.convertToAnyShouldWrapper

class EquivalentTest extends ClassicSharedTests  {
  test("ZeroCodeGen with lits should CSE") {

    val equiv = new EquivalentExpressions()
    val zcgs = Seq.fill(4)(ZeroCodeGen(Literal(null, BooleanType), Literal(null, BooleanType)))

    zcgs.foreach(equiv.addExprTree(_))

    equiv.getCommonSubexpressions.size shouldBe 1
  }

  test("Runners should CSE") {

    val equiv = new EquivalentExpressions()
    val zcgs = Seq.fill(4)(ShimUtils.expression(ruleRunner(
      RuleSuite(Id(1,1), Seq(RuleSet(Id(2,1), Seq(
        Rule(Id(1,1), ExpressionRule("3 > 2")
        )))))
    )))

    zcgs.foreach(equiv.addExprTree(_))

    equiv.getCommonSubexpressions.size shouldBe 1
  }

  test("ZeroCodeGen with runners should CSE") {

    val equiv = new EquivalentExpressions()
    val zcgs = Seq.fill(4)(ZeroCodeGen(Literal(null, BooleanType), ShimUtils.expression(ruleRunner(
      RuleSuite(Id(1,1), Seq(RuleSet(Id(2,1), Seq(
        Rule(Id(1,1), ExpressionRule("3 > 2")
        )))))
    )), wrapped = true))

    zcgs.foreach(equiv.addExprTree(_))

    equiv.getCommonSubexpressions.size shouldBe 1
  }

  test("Conditionals with nested runners _could_ CSE but won't as only one will be called") {
// this does cost on init per partition but doesn't affect runtime
    val equiv = new EquivalentExpressions()
    val zcg = ZeroCodeGen(Literal(null, BooleanType), ShimUtils.expression(ruleRunner(
      RuleSuite(Id(1,1), Seq(RuleSet(Id(2,1), Seq(
        Rule(Id(1,1), ExpressionRule("3 > 2")
        )))))
    )), wrapped = true)

    val cw = CaseWhen(Seq(
      (EqualTo(BoundReference(3, IntegerType, false), Literal(3, IntegerType)), zcg),
      (EqualTo(BoundReference(1, IntegerType, false), Literal(4, IntegerType)), zcg),
      (EqualTo(BoundReference(2, IntegerType, false), Literal(5, IntegerType)), zcg),
    ), Some(zcg))
    equiv.addExprTree(cw)

    //  CaseWhen above won't trigger for values unless an else is provided
    equiv.getCommonSubexpressions.size shouldBe 0
  }

}
