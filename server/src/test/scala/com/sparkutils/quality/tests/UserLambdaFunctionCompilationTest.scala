package com.sparkutils.quality.tests

import com.sparkutils.quality._
import com.sparkutils.quality.impl.extension.{FunNRewriteBase, QualitySparkExtension}
import com.sparkutils.testing.Testing
import com.sparkutils.quality.tests.TestHandler._
import com.sparkutils.qualityTests.util.ClassicSharedTests
import org.apache.spark.sql.catalyst.expressions.{Alias, ArrayFilter, CreateArray, ExprId, Expression, Flatten, Literal, NamedLambdaVariable, ZipWith}
import org.apache.spark.sql.qualityFunctions.LambdaCompilationUtils.{LambdaCompilationHandler, convertToCompilationHandlers, envLambdaHandlers, loadLambdaCompilationHandlers}
import org.apache.spark.sql.qualityFunctions.{DoCodegenFallbackHandler, FunN, NamedLambdaVariableCodeGen}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers.convertToAnyShouldWrapper

import java.util.concurrent.atomic.AtomicBoolean

class UserLambdaFunctionCompilationTest extends ClassicSharedTests with BeforeAndAfterAll {

  test("defaultHofConfigTests") {
    val (simple, simpleimpl, notso, complex) = ("simple", "simpleimpl", "not.so.simple", "complex")
    val expected = Map(simple -> simpleimpl, notso -> complex)
    // verify default loading
    val got = envLambdaHandlers(s" $simple = $simpleimpl , $notso=  $complex  ")
    assert(got == expected)
  }

  test("loadHandlers") {
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

  test("convertHandlers") {
    try {
      convertToCompilationHandlers(loadLambdaCompilationHandlers(Map(classOf[ZipWith].getName -> classOf[TestMe].getName)))
      assert(false, "Should have thrown")
    } catch {
      case e: Exception => assert(e.getMessage.indexOf("does not implement LambdaCompilationHandler") > 0)
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    System.clearProperty("quality.lambdaHandlers")
  }

  test("loadHandlersViaProperty") {
    System.setProperty("quality.lambdaHandlers", s"${classOf[ZipWith].getName}=${classOf[ZipWith].getName}")
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
      LambdaFunction("bottom", "i -> i + 1", Id(1, 3)),
      LambdaFunction("top", "(i, j) -> bottom(i)", Id(1, 3))
    ))
    val s = sparkSession
    import s.implicits._
    val df = sparkSession.sql("select top(1,2)")
    assert(df.as[Integer].collect().head == 2)
  }

  test("runDisabledCompilation") { evalCodeGens {
    funNRewrites {
      System.setProperty("quality.lambdaHandlers", s"${classOf[FunN].getName}=${classOf[DoCodegenFallbackHandler].getName}")
      doSimpleNested
    }
  } }

  test("runNestedCompilation") { evalCodeGens {
    funNRewrites {
      doSimpleNested
    }
  } }

  def doWithFilterHof: Unit = {
    /**
     * NOTE this can only exercise the code it can't test it's called DoCodeGen properly
     */
    registerLambdaFunctions(Seq(
      LambdaFunction("bottom", "filterB -> filter(filterB, i -> i % 2 = 0)", Id(1, 3)),
      LambdaFunction("top", "a -> bottom(a)", Id(1, 3))
    ))
    val s = sparkSession
    import s.implicits._
    val df = sparkSession.sql("select element_at(top(array(1,2)), 1)")
    assert(df.as[Integer].collect().head == 2)
  }

  test("withDefaultHoF") { evalCodeGens {
    funNRewrites {
      doWithFilterHof
    }
  } }

  test("withSpecifiedHoFHandler") { evalCodeGens {
    funNRewrites {
      sparkSession.sparkContext.setLocalProperty("quality.lambdaHandlers", s"${classOf[ArrayFilter].getName}=${classOf[DoCodegenFallbackHandler].getName}")
      doWithFilterHof
    }
  } }

  test("runDisabledBottom") { forceCodeGen {

    def doIt(clearIt: Boolean = false) = {
      reinit()

      if (clearIt)
        System.clearProperty("quality.lambdaHandlers")
      else
        System.setProperty("quality.lambdaHandlers", s"bottom=${classOf[TestHandler].getName}")

      doSimpleNested
    }

    v3_2_and_above {
      justfunNRewrite {

        doIt()

        not_Cluster {
          // the lambdas will not have been re-written due to the handler
          calledShouldTransform.get shouldBe true
          calledTransform.get shouldBe true
        }
      }
    }

    v3_2_and_above {
      justfunNRewrite {

        doIt(clearIt = true)

        not_Cluster {
          // the lambdas should have been re-written out of existence as we've specified a handler
          // no FunN means no lambda to handle
          calledShouldTransform.get shouldBe false
          calledTransform.get shouldBe false
        }
      }
    }

    {

      doIt()

      not_Cluster {
        // no re-writes means lambdas are still there so handlers are called
        // we'll functionally test it but these are set on other jvms
        calledShouldTransform.get shouldBe true
        calledTransform.get shouldBe true
      }
    }
  } }

  // FunNRewriteD needed to allow swapping the config out to test disabling the entire plugin
  lazy val justfunNRewriteD = testPlan(FunNRewriteD, secondRunWithoutPlan = false) _

  test("disabledRewriteNestedArray") { v3_2_and_above { justfunNRewriteD {

    val before = System.getProperty(QualitySparkExtension.disableRulesConf)
    try {
      System.setProperty(QualitySparkExtension.disableRulesConf, FunNRewriteD.className)

      val toarr = LambdaFunction("toarr", "(a, b) -> array(a, b)", Id(1, 2))
      val toarr2 = LambdaFunction("toarr2", "(a, b) -> flatten(array(array(b, a, b), toarr(a,b)))", Id(1, 2))
      registerLambdaFunctions(Seq(toarr, toarr2))

      val s = sparkSession
      import s.implicits._

      val ds = sparkSession.sql("select toarr2(1,2) as o").as[Seq[Int]]

      val a = ds.queryExecution.executedPlan.collect {
        case p => p.expressions.collect {
          case a: Alias if a.name == "o" => a
        }
      }.flatten

      assert(a.size == 1)
      // all rewrites should be disabled, so FunN should be present
      assert(a.head.child match {
        case _: FunN => true
        case _ => false
      })

      val threeUsages = ds.head()
      assert(threeUsages == Seq(2, 1, 2, 1, 2))

    } finally {
      if (before eq null)
        System.clearProperty(QualitySparkExtension.disableRulesConf)
      else
        System.setProperty(QualitySparkExtension.disableRulesConf, before)
    }
  } } }

  test("rewriteNestedArray") { v3_2_and_above { justfunNRewrite {
    val toarr = LambdaFunction("toarr", "(a, b) -> array(a, b)", Id(1,2))
    val toarr2 = LambdaFunction("toarr2", "(a, b) -> flatten(array(array(b, a, b), toarr(a,b)))", Id(1,2))
    registerLambdaFunctions(Seq(toarr, toarr2))

    val s = sparkSession
    import s.implicits._

    val ds = sparkSession.sql("select toarr2(1,2) as o").as[Seq[Int]]

    val a = ds.queryExecution.executedPlan.collect{
      case p => p.expressions.collect {
        case a: Alias if a.name == "o" => a
      }
    }.flatten

    assert(a.size == 1)
    // where both swapped out?
    assert(a.head.child match {
      case f: Flatten if f.children.size == 1 && f.children.head.isInstanceOf[CreateArray] =>
        f.children.head.children match {
          case Seq(a: CreateArray, b: CreateArray) if a.children.size == 3 && b.children.size == 2 =>
            (
              a.children match {
                case Seq(a: Literal, b: Literal, c: Literal) if a.value == 2 && b.value == 1 && c.value == 2 => true
                case _ => false
              }
              ) && (
              b.children match {
                case Seq(a: Literal, b: Literal) if a.value == 1 && b.value == 2 => true
                case _=> false
              }
              )
          case _ => false
        }
      case _ => false
    })

    val threeUsages = ds.head()
    assert(threeUsages == Seq(2,1,2,1,2))
  } } }

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

// only difference is disabled is always evaluated so disabling can be tested
object FunNRewriteD extends FunNRewriteBase {

  override def className = "com.sparkutils.quality.tests.FunNRewriteD"

  def disabled: Boolean = shouldBeDisabled

}