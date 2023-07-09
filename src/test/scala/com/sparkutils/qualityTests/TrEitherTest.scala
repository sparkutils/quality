package com.sparkutils.qualityTests
import com.sparkutils.quality.Id
import org.junit.Test
import com.sparkutils.quality.impl.util.RuleSuiteDocs._
import org.junit.Assert.fail

class TrEitherTest {
  val lambda = LambdaId(Id(1,2))
  val output = OutputExpressionId(Id(2,2))
  val rule = RuleId(Id(3,2))

  def assertNotImplemented[A](idTrEither: => A) = {
    try{
      val r = idTrEither
      fail("should have thrown")
    } catch {
      case t: scala.NotImplementedError => () // this we expect
    }
  }

  @Test
  def testIs: Unit = {
    assert(lambda.isA)
    assert(!lambda.isB)
    assert(!lambda.isC)

    assert(!output.isA)
    assert(output.isB)
    assert(!output.isC)

    assert(!rule.isA)
    assert(!rule.isB)
    assert(rule.isC)
  }

  @Test
  def testGets: Unit = {

    assert(lambda.getA == Id(1,2))
    assertNotImplemented(lambda.getB)
    assertNotImplemented(lambda.getC)

    assert(output.getB == Id(2,2))
    assertNotImplemented(output.getA)
    assertNotImplemented(output.getC)

    assert(rule.getC == Id(3,2))
    assertNotImplemented(rule.getA)
    assertNotImplemented(rule.getB)
  }

  @Test
  def testFolds: Unit = {
    assert(lambda.fold(identity,identity,identity) == Id(1,2))
    assert(output.fold(identity,identity,identity) == Id(2,2))
    assert(rule.fold(identity,identity,identity) == Id(3,2))
  }
}
