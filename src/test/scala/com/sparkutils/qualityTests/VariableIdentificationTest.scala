package com.sparkutils.qualityTests

import com.sparkutils.quality.impl.{ExpressionLookup, VariablesLookup}
import com.sparkutils.quality.Id
import org.apache.spark.sql.QualitySparkUtils.newParser
import org.apache.spark.sql.catalyst.expressions.Expression
import org.junit.Test
import org.scalatest.FunSuite

class VariableIdentificationTest extends FunSuite with TestUtils {

  val parser = newParser()

  @Test
  def testSimpleLambdaLookup: Unit = evalCodeGensNoResolve {
    val expr = parser.parseExpression("(a, b) -> a + b + d + a2n")
    val id = Id(1,1)
    // we expect only d and a2n to return as they are part of the outer scope and not provided by this lambda
    val (res, oids, unknown) = VariablesLookup.processLambdas(Map("test" -> Map(id -> expr)))

    assert(res == Map("test" -> Map(id -> Set("d", "a2n"))))
  }

  @Test
  def testSimpleLambdaLookupWithFunctions: Unit = evalCodeGensNoResolve {
    val expr = parser.parseExpression("(a, b) -> a + concat(b,d) + a2n")
    val id = Id(1,1)
    // we expect only d and a2n to return as they are part of the outer scope and not provided by this lambda
    val (res, oids, unknown) = VariablesLookup.processLambdas(Map("test" -> Map(id -> expr)))

    assert(res == Map("test" -> Map( id -> Set("d", "a2n"))))
  }

  @Test
  def testSimpleLambdaLookupWithUnknownFunctions: Unit = evalCodeGensNoResolve {
    val expr = parser.parseExpression("(a, b) -> a + conflat(b,d) + purple(a2n)")
    val id = Id(1,1)
    // we expect only d and a2n to return as they are part of the outer scope and not provided by this lambda
    val (res, oids, unknown) = VariablesLookup.processLambdas(Map("test" -> Map(id -> expr)))

    assert(res == Map("test" -> Map( id -> Set("d", "a2n"))))
    assert(unknown == Map( id -> Set("conflat","purple")))
  }

  def doTestNestedAndNonEvaluatedLambda(f: Seq[(String, Map[Id, Expression])] => Map[String, Map[Id, Expression]])= evalCodeGensNoResolve {
    val funcExpr = parser.parseExpression("(a1, b1) -> a1 + concat(b1,e) + f")
    val id = Id(1,1)
    val expr = parser.parseExpression("(a, b) -> a + func(b,d) + a2n")
    val id2 = Id(2,1)

    // we expect only d and a2n and e and f to return as they are part of the outer scope and not provided by these lambdas
    val (res, oids, unknown) = VariablesLookup.processLambdas(f(Seq("test" -> Map(id2 -> expr), "func" -> Map(id -> funcExpr))))

    assert(res == Map("test" -> Map(id2 -> Set("d", "a2n")), "func" -> Map(id -> Set("e", "f"))))
  }

  @Test
  def testNestedAndNonEvaluatedLambda: Unit = doTestNestedAndNonEvaluatedLambda{
    s =>
      Map(s.head, s.last)
  }

  @Test
  def testNestedAndNonEvaluatedLambdaReversedOrder: Unit = doTestNestedAndNonEvaluatedLambda{
    s =>
      Map(s.last, s.head)
  }

  @Test
  def testOverloadedLambdas: Unit = evalCodeGensNoResolve {
    val funcExpr = parser.parseExpression("(a1, b1) -> a1 + concat(b1,e) + f")
    val id = Id(1,1)
    val expr = parser.parseExpression("(a, b) -> a + concat(b,d) + a2n")
    val id2 = Id(2,1)
    // we expect only d and a2n and e + f to return as they are part of the outer scope and not provided by these lambdas
    // but they should be in the correct id's
    val (res, oids, unknown) = VariablesLookup.processLambdas(Map("func" -> Map(id -> funcExpr, id2 -> expr)))

    assert(res == Map("func" -> Map(id -> Set("e", "f"), id2 -> Set("d", "a2n"))))
  }

  @Test
  def testNonLambdaNonLeaf: Unit = evalCodeGensNoResolve {
    val expr = parser.parseExpression("a + concat(b,d) + a2n")
    val id = Id(1,1)
    // we don't expect anything here as there isn't a lambda
    val (res, oids, unknown) = VariablesLookup.processLambdas(Map("test" -> Map(id -> expr)))

    // not a lambda, but will force children
    assert(res == Map("test" -> Map(Id(1,1) -> Set("a", "a2n", "b", "d"))))
  }

  @Test
  def testLambdaFromANonLeaf: Unit = evalCodeGensNoResolve {
    val expr = parser.parseExpression("a + concat((e,f) -> b,d) + a2n")
    val id = Id(1,1)
    // we don't expect anything as it's not a lambda we are aware of
    val (res, oids, unknown) = VariablesLookup.processLambdas(Map("test" -> Map(id -> expr)))

    // the nested lambda does use a non variable
    assert(res == Map("test" -> Map(Id(1,1) -> Set("a", "a2n", "b", "d"))))
  }

  @Test
  def testFieldLookupWithLambda: Unit = evalCodeGensNoResolve {
    val lambdas = Map("test" -> Map(Id(0,1) -> Set("d", "a2n", "e", "f", "z")), "func" -> Map(Id(1,1) -> Set("e", "f")))
    val expr = parser.parseExpression("a + func(b,d) + a2n + unknownFunc() + concat(a, a2n)")
    // we expect a and a2n, b and d as they are in the direct expression, then e and f as they are in func
    val ExpressionLookup(res, unknown, usedLambdas, known) = VariablesLookup.fieldsFromExpression(expr, lambdas)

    // z is part of test not func
    assert(res == Set("d", "a2n", "e", "f", "a", "b"))
    assert(usedLambdas == Set(Id(1,1)))
    assert(unknown == Set("unknownFunc"))
    assert(known == Set("concat"))
  }
}
