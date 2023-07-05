package com.sparkutils.quality.tests

import com.sparkutils.quality._
import com.sparkutils.quality.impl.util.Testing
import com.sparkutils.quality.tests.TestHandler._
import com.sparkutils.qualityTests.{RowTools, SparkTestUtils, TestUtils}
import org.apache.spark.sql.catalyst.expressions.{ArrayFilter, ExprId, Expression, NamedLambdaVariable, ZipWith}
import org.apache.spark.sql.qualityFunctions.LambdaCompilationUtils.{LambdaCompilationHandler, convertToCompilationHandlers, envLambdaHandlers, loadLambdaCompilationHandlers}
import org.apache.spark.sql.qualityFunctions.{DoCodegenFallbackHandler, FunN, NamedLambdaVariableCodeGen}
import org.junit.{Before, Test}
import org.scalatest.FunSuite

import java.util.concurrent.atomic.AtomicBoolean

class UserLambdaFunctionCompilationTest extends FunSuite with TestUtils {
  // le horrible hack for testing
  Testing.setTesting()

  @Test
  def defaultHofConfigTests: Unit = {
    val (simple, simpleimpl, notso, complex) = ("simple","simpleimpl","not.so.simple","complex")
    val expected = Map(simple -> simpleimpl, notso -> complex)
    // verify default loading
    val got = envLambdaHandlers(s" $simple = $simpleimpl , $notso=  $complex  ")
    assert(got == expected)
  }

  @Test
  def loadHandlers: Unit = {
    val handlers = Map(classOf[ZipWith].getName -> classOf[DoCodegenFallbackHandler].getName)
    val res1 = loadLambdaCompilationHandlers(handlers)
    assert(res1.head._2.isInstanceOf[DoCodegenFallbackHandler])
    try {
      loadLambdaCompilationHandlers(Map(classOf[ZipWith].getName -> "funtoload"))
      assert(false, "Should have thrown")
    } catch {
      case e: Exception => assert(e.getMessage.indexOf("funtoload cannot be found") > 0)
    }
  }

  @Test
  def convertHandlers: Unit = {
    try {
      convertToCompilationHandlers(loadLambdaCompilationHandlers(Map(classOf[ZipWith].getName -> classOf[TestMe].getName)))
      assert(false, "Should have thrown")
    } catch {
      case e: Exception => assert(e.getMessage.indexOf("does not implement LambdaCompilationHandler") > 0)
    }
  }

  @Before
  override def setup(): Unit = {
    super.setup()
    System.clearProperty("quality.lambdaHandlers")
  }

  @Test
  def loadHandlersViaProperty: Unit = {
    System.setProperty("quality.lambdaHandlers",s"${classOf[ZipWith].getName}=${classOf[ZipWith].getName}")
    try {
      convertToCompilationHandlers()
      assert(false, "Should have thrown")
    } catch {
      case e: Exception => assert(e.getMessage.indexOf("has no default constructor") > 0)
    }
  }

  def doSimpleNested = {

    /**
     * NOTE this can only exercise the code it can't test it's called DoCodeGen properly
     */
    registerLambdaFunctions(Seq(
      LambdaFunction("bottom","i -> i + 1",Id(1,3)),
      LambdaFunction("top","(i, j) -> bottom(i)",Id(1,3))
    ))
    import sparkSession.implicits._
    val df = sparkSession.sql("select top(1,2)")
    assert(df.as[Integer].collect.head == 2)
  }

  @Test
  def runDisabledCompilation: Unit = evalCodeGens {
    System.setProperty("quality.lambdaHandlers",s"${classOf[FunN].getName}=${classOf[DoCodegenFallbackHandler].getName}")
    doSimpleNested
  }

  @Test
  def runNestedCompilation: Unit = evalCodeGens {
    doSimpleNested
  }

  def doWithFilterHof: Unit = {
    if (SparkTestUtils.skipHofs && !onDatabricks) return
    // we can't run tests on 9.1.dbr build, but can on runtime

    /**
     * NOTE this can only exercise the code it can't test it's called DoCodeGen properly
     */
    registerLambdaFunctions(Seq(
      LambdaFunction("bottom","filterB -> filter(filterB, i -> i % 2 = 0)",Id(1,3)),
      LambdaFunction("top","a -> printCode(bottom(a))",Id(1,3))
    ))
    import sparkSession.implicits._
    val df = sparkSession.sql("select element_at(top(array(1,2)), 1)")
    assert(df.as[Integer].collect.head == 2)
  }

  @Test
  def withDefaultHoF: Unit = evalCodeGens {
    doWithFilterHof
  }

  @Test
  def withSpecifiedHoFHandler: Unit = evalCodeGens {
    sparkSession.sparkContext.setLocalProperty("quality.lambdaHandlers",s"${classOf[ArrayFilter].getName}=${classOf[DoCodegenFallbackHandler].getName}")
    doWithFilterHof
  }

  @Test
  def runDisabledBottom: Unit = forceCodeGen {

    reinit()
    System.setProperty("quality.lambdaHandlers",s"bottom=${classOf[TestHandler].getName}")
    doSimpleNested

    if (!onDatabricks) {
      // we'll functionally test it but these are set on other jvms
      assert(calledShouldTransform.get, true)
      assert(calledTransform.get, true)
    }
  }

}

object TestHandler {
  val calledShouldTransform = new AtomicBoolean(false)
  val calledTransform = new AtomicBoolean(false)

  def reinit(): Unit = {
    calledShouldTransform.set(false)
    calledTransform.set(false)
  }

}

case class TestHandler() extends LambdaCompilationHandler {

  /**
   *
   * @param expr
   * @return empty if the expression should be transformed (i.e. there is a custom solution for it).  Otherwise return the full set of NamedLambdaVariables found
   */
  def shouldTransform(expr: Expression): Seq[NamedLambdaVariable] = {
    calledShouldTransform.set( true )
    expr.collect{
      case exp: NamedLambdaVariable => exp
    }
  }

  /**
   * Transform the expression using the scope of replaceable named lambda variable expression
   *
   * @param expr
   * @param scope
   * @return
   */
  def transform(expr: Expression, scope: Map[ExprId, NamedLambdaVariableCodeGen]): Expression = {
    calledTransform.set( true )
    expr
  }
}

class TestMe() {

}