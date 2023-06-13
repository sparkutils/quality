package com.sparkutils.quality.tests

import com.sparkutils.quality._
import com.sparkutils.quality.impl.{RuleRegistrationFunctions, RuleRunnerUtils}
import com.sparkutils.qualityTests.TestUtils
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.StringType
import org.junit.Test
import org.scalatest.FunSuite

class RoundTripPrivateTest extends FunSuite with TestUtils {

  @Test
  def ruleExprSwapping(): Unit = evalCodeGens {

    case class LitRule(string: String) extends ExprLogic {
      val expr = Literal.create(string, StringType)
    }

    val rules = RuleSuite(Id(1,1), Seq(
      RuleSet(Id(50, 1), Seq(
        Rule(Id(100, 1), LitRule("a")),
        Rule(Id(100, 2), LitRule("b")),
        Rule(Id(100, 3), LitRule("c")),
        Rule(Id(100, 4), LitRule("d"))
      )),
      RuleSet(Id(50, 2), Seq(
        Rule(Id(100, 5), LitRule("e")),
        Rule(Id(100, 6), LitRule("f")),
        Rule(Id(100, 7), LitRule("g")),
        Rule(Id(100, 8), LitRule("h"))
      )),
      RuleSet(Id(50, 3), Seq(
        Rule(Id(100, 9), LitRule("i")),
        Rule(Id(100, 10), LitRule("j")),
        Rule(Id(100, 11), LitRule("k")),
        Rule(Id(100, 12), LitRule("l"))
      ))
    ))

    val flatABC = RuleRegistrationFunctions.flattenExpressions(rules)
    assert( flatABC.map( _ match {
      case Literal(a, StringType) => a
    }).mkString("") == "abcdefghijkl", "no alphabet" )

    val itr = "123456789012".iterator
    val converted = flatABC.map( _ => LitRule(itr.next().toString).expr)

    val reincorporated = RuleRunnerUtils.reincorporateExpressions(rules, converted)

    val expected = RuleSuite(Id(1,1), Seq(
      RuleSet(Id(50, 1), Seq(
        Rule(Id(100, 1), ExpressionWrapper(Literal.create("1",StringType))),
        Rule(Id(100, 2), ExpressionWrapper(Literal.create("2",StringType))),
        Rule(Id(100, 3), ExpressionWrapper(Literal.create("3",StringType))),
        Rule(Id(100, 4), ExpressionWrapper(Literal.create("4",StringType)))
      )),
      RuleSet(Id(50, 2), Seq(
        Rule(Id(100, 5), ExpressionWrapper(Literal.create("5",StringType))),
        Rule(Id(100, 6), ExpressionWrapper(Literal.create("6",StringType))),
        Rule(Id(100, 7), ExpressionWrapper(Literal.create("7",StringType))),
        Rule(Id(100, 8), ExpressionWrapper(Literal.create("8",StringType)))
      )),
      RuleSet(Id(50, 3), Seq(
        Rule(Id(100, 9), ExpressionWrapper(Literal.create("9",StringType))),
        Rule(Id(100, 10), ExpressionWrapper(Literal.create("0",StringType))),
        Rule(Id(100, 11), ExpressionWrapper(Literal.create("1",StringType))),
        Rule(Id(100, 12), ExpressionWrapper(Literal.create("2",StringType)))
      ))
    ))

    assert( expected == reincorporated, "didn't come back alive" )
  }

}
