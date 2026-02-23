package com.sparkutils.qualityTests.classicOnly

import com.sparkutils.quality.{ExpressionRule, Id, OutputExpression, QualityException, RuleSuiteGroup, RuleSuiteGroupResults, RunOnPassProcessor, register_rule_suite_group}
import com.sparkutils.quality.impl.OfRuleSuite
import com.sparkutils.qualityTests.RuleEngineTest.{rulesRaw, testData}
import com.sparkutils.qualityTests.util.ClassicSharedTests
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.BinaryType
import org.scalatest.Matchers

class ServerSideTests extends ClassicSharedTests with Matchers {

  test("null shouldn't match") {
    null match {
      case OfRuleSuite(_) => fail("Should not have matched")
      case _ => ()
    }
  }

  test("literal of string shouldn't match") {
    Literal("str") match {
      case OfRuleSuite(_) => fail("Should not have matched")
      case _ => ()
    }
  }

  test("bad bytes should throw") {
    val caught =
      intercept[QualityException] { // Result type: IndexOutOfBoundsException


      Literal(Array.ofDim[Byte](22), BinaryType) match {
        case OfRuleSuite(_) => fail("Should not have matched")
        case _ => ()
      }

    }

    caught.msg should include("Could not deserialize")
  }


  def group = RuleSuiteGroup(
    Set(
      rulesRaw(
        Seq((ExpressionRule("product = 'edt' and subcode = 40"), RunOnPassProcessor(1000, Id(1040, 1),
          OutputExpression("array(account_row('from'), account_row('to', 'other_account1'))"))),
          (ExpressionRule("product like '%fx%'"), RunOnPassProcessor(1000, Id(1042, 1),
            OutputExpression("array(named_struct('transfer_type', 'from', 'account', 'another_account', 'product', product, 'subcode', subcode), named_struct('transfer_type', 'to', 'account', account, 'product', product, 'subcode', subcode))"))),
          (ExpressionRule("product = 'eqotc'"), RunOnPassProcessor(1000, Id(1043, 1),
            OutputExpression("array(subcode('fromWithField', 6000), account_row('to', 'other_account1'))")))
        )
      ).copy(id = Id(1,1)),
      rulesRaw(
        Seq((ExpressionRule("product = 'edt' and subcode = 40"), RunOnPassProcessor(1000, Id(1040, 1),
          OutputExpression("array(account_row('from'), account_row('to', 'other_account1'))"))),
          (ExpressionRule("product like '%fx%'"), RunOnPassProcessor(1000, Id(1042, 1),
            OutputExpression("array(named_struct('transfer_type', 'from', 'account', 'another_account', 'product', product, 'subcode', subcode), named_struct('transfer_type', 'to', 'account', account, 'product', product, 'subcode', subcode))"))),
          (ExpressionRule("product = 'eqotc'"), RunOnPassProcessor(1000, Id(1043, 1),
            OutputExpression("array(subcode('fromWithField', 6000), account_row('to', 'other_account1'))")))
        )
      ).copy(id = Id(2,1)),
      rulesRaw(
        Seq((ExpressionRule("product = 'edt' and subcode = 40"), RunOnPassProcessor(1000, Id(1040, 1),
          OutputExpression("array(account_row('from'), account_row('to', 'other_account1'))"))),
          (ExpressionRule("product like '%fx%'"), RunOnPassProcessor(1000, Id(1042, 1),
            OutputExpression("array(named_struct('transfer_type', 'from', 'account', 'another_account', 'product', product, 'subcode', subcode), named_struct('transfer_type', 'to', 'account', account, 'product', product, 'subcode', subcode))"))),
          (ExpressionRule("product = 'eqotc'"), RunOnPassProcessor(1000, Id(1043, 1),
            OutputExpression("array(subcode('fromWithField', 6000), account_row('to', 'other_account1'))")))
        )
      ).copy(id = Id(3,1))
    )
  )

  test("dq results should group") {
    val name = register_rule_suite_group(group)
    val s = sparkSession
    import s.implicits._
    val tds = testData.toDS()
    val ds = tds.selectExpr(s"group_results( array( dq_rule_runner(rule_suite_from($name, 1)), " +
      s"dq_rule_runner(rule_suite_from($name, 2)), dq_rule_runner(rule_suite_from($name, 3)) ) ) as res")
    //ds.show()
    import com.sparkutils.quality.implicits._
    val r = ds.selectExpr("res.*").as[RuleSuiteGroupResults].collect()
    r.map(_.ruleSuiteResults.keys.toSeq).distinct shouldBe Seq(
      Seq(Id(1,1), Id(2,1), Id(3,1))
    )
  }
}
