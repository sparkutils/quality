package com.sparkutils.qualityTests

import org.apache.spark.sql.{Column, ShimUtils, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.junit.{Before, Test}
import org.scalatest.FunSuite
import com.sparkutils.quality.{LambdaFunction, _}
import com.sparkutils.quality.impl.ExpressionRunner
import com.sparkutils.quality.impl.RuleLogicUtils.mapRules
import com.sparkutils.quality.impl.extension.FunNRewrite
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types.{BooleanType, DataType, IntegerType}
import org.scalatest.Matchers.convertToAnyShouldWrapper

import java.util.concurrent.atomic.AtomicInteger

/**
 * tests if sub expressions are properly eliminated in the various runner configurations
 */
class SubExpressionEliminationTest extends FunSuite with TestUtils {

  val data = Seq(
    TestOn("p1","a1", 0),
    TestOn("p2","a2", 1),
    TestOn("p2","a1", 1),
    TestOn("p1","a2", 0)
  )

  val triggers = Seq(
    Rule(Id(1,1), ExpressionRule("myequal(product, 'p1') and myequal2(subcode, 1)")),
    Rule(Id(2,1), ExpressionRule("myequal(product, 'p1') and myequal2(subcode, 0)")),
    Rule(Id(3,1), ExpressionRule("myequal(product, 'p2') and myequal2(subcode, 1)")),
    Rule(Id(4,1), ExpressionRule("myequal(product, 'p2') and myequal2(subcode, 0)")),
    Rule(Id(5,1), ExpressionRule("myequal(product, 'p1') and myequal2(account, 'a1')")),
    Rule(Id(6,1), ExpressionRule("myequal(product, 'p1') and myequal2(account, 'a2')")),
    Rule(Id(7,1), ExpressionRule("myequal(product, 'p2') and myequal2(account, 'a1')")),
    Rule(Id(8,1), ExpressionRule("myequal(product, 'p2') and myequal2(account, 'a2')"))
  )

  val lambdas = Seq(
    LambdaFunction("myequal2","(a, b) -> myequal3(a,b)",Id(1,1)),
    LambdaFunction("myequal3","(a, b) -> myequal(a,b)",Id(2,1))
  )

  lazy val registerFunction: (String, Seq[Expression] => Expression) => Unit =
    ShimUtils.registerFunction(SparkSession.getActiveSession.get.sessionState.functionRegistry) _

  @Before
  def resetCountAndRegister(): Unit = {
    // no-op to force it to be created (parent hasn't happened yet)
    sparkSession.conf

    EqualToTest.counter.set(0)
    registerFunction("myequal", exprs => EqualToTest(exprs.head, exprs.last))
  }

  // can't do cluster runs as this is local vm only
  def doRunner(count: Int, rsf: RuleSuite => Column): Unit = not_Cluster{
    EqualToTest.counter.set(0)
    val rs = RuleSuite(Id(1,1), Seq(RuleSet(Id(1,1), triggers)), lambdaFunctions = lambdas)
    import sparkSession.implicits._
    val res = data.toDS().withColumn("dq", rsf(rs)).collect()
    val cur = EqualToTest.counter.get()
    /*import quality.implicits._
    val rres = data.toDS().withColumn("dq", rsf(rs)).selectExpr("dq.*").as[GeneralExpressionsResult[Int]].collect()*/
    cur shouldBe count
  }

  val rows = data.size
  val expectedTriggerRules = 12*rows // (4 rows, 12 times called per each)
  val expectedEliminatedTriggerRules = 6*rows // 4 rows, 6 _unique_

  @Test
  def controlRunner(): Unit = evalCodeGensNoResolve{ doRunner(expectedTriggerRules, ruleRunner(_)) }  // defaults may change later

  // forceRunnerEval disables codegen elimination as CodeGenFallback is also ignored for interpreted
  @Test
  def runnerShouldNotEliminateWithRunnerEval(): Unit = evalCodeGensNoResolve { doRunner(expectedTriggerRules, ruleRunner(_, compileEvals = false, forceRunnerEval = true)) }

  @Test
  def runnerShouldEliminate(): Unit = v3_2_and_above { evalCodeGensNoResolve { doRunner(expectedEliminatedTriggerRules, ruleRunner(_, compileEvals = false)) } }

  // adds an output expression
  def doOutput(count: Int, rsf: RuleSuite => Column, expr: String): Unit =
    doRunner(count, irs => rsf(mapRules(irs)(
      _.copy(runOnPassProcessor = RunOnPassProcessor(1000, Id(1042,1),OutputExpression(expr)))
    )))


  val outputExpr = "if(myequal(product, 'p1'), 1, 0)"

  val expectedOutputRules = rows // one for each row is extra called

  @Test
  def controlEngine(): Unit = evalCodeGensNoResolve{ doOutput(expectedTriggerRules + expectedOutputRules, ruleEngineRunner(_, IntegerType), outputExpr) }  // defaults may change later

  // forceRunnerEval disables codegen elimination as CodeGenFallback is also ignored for interpreted
  @Test
  def engineShouldNotEliminateWithRunnerEval(): Unit = evalCodeGensNoResolve { doOutput(expectedTriggerRules + expectedOutputRules, ruleEngineRunner(_, IntegerType, compileEvals = false, forceRunnerEval = true), outputExpr) }

  // note there should be no more calls as the outputexpr is already eliminated
  @Test
  def engineShouldEliminate(): Unit = v3_2_and_above { evalCodeGensNoResolve{ doOutput(expectedEliminatedTriggerRules, ruleEngineRunner(_, IntegerType, forceTriggerEval = false, compileEvals = false), outputExpr) } }

  @Test
  def controlExpression(): Unit = evalCodeGensNoResolve{ doRunner(expectedTriggerRules, ExpressionRunner(_, ddlType = "boolean")) }  // defaults may change later

  // forceRunnerEval disables codegen elimination as CodeGenFallback is also ignored for interpreted
  @Test
  def expressionShouldNotEliminateWithRunnerEval(): Unit = evalCodeGensNoResolve { doRunner(expectedTriggerRules, ExpressionRunner(_, ddlType = "boolean", compileEvals = false, forceRunnerEval = true)) }

  // note there should be no more calls as the outputexpr is already eliminated
  @Test
  def expressionShouldEliminate(): Unit = v3_2_and_above { evalCodeGensNoResolve { doRunner(expectedEliminatedTriggerRules, ExpressionRunner(_, ddlType = "boolean", compileEvals = false)) } }

  val folderExpr = "a -> if(myequal(product, 'p1'), a, named_struct('r',0))"
  val starter = sql.functions.struct(sql.functions.lit(1).as("r"))
  val folderOverhead = rows // not entirely sure why

  @Test
  def controlFolder(): Unit = evalCodeGensNoResolve{ doOutput(expectedTriggerRules + expectedOutputRules + folderOverhead, ruleFolderRunner(_, starter), folderExpr) }  // defaults may change later

  // forceRunnerEval disables codegen elimination as CodeGenFallback is also ignored for interpreted
  @Test
  def folderShouldNotEliminateWithRunnerEval(): Unit = evalCodeGensNoResolve { doOutput(expectedTriggerRules + expectedOutputRules + folderOverhead, ruleFolderRunner(_, starter, compileEvals = false, forceRunnerEval = true), folderExpr) }

  // note there should be no more calls as the outputexpr is already eliminated
  @Test
  def folderShouldEliminate(): Unit = v3_2_and_above { evalCodeGensNoResolve{ doOutput(expectedEliminatedTriggerRules, ruleFolderRunner(_, starter, compileEvals = false), folderExpr) } }

  @Test
  def folderShouldEliminateWithTriggersFalse(): Unit = v3_2_and_above { evalCodeGensNoResolve{ doOutput(expectedEliminatedTriggerRules, ruleFolderRunner(_, starter, compileEvals = false, forceTriggerEval = false), folderExpr) } }

}

object EqualToTest {
  val counter = new AtomicInteger(0)
  def inc(): Unit = counter.incrementAndGet()
}

// EqualTo that triggers counter
case class EqualToTest(left: Expression, right: Expression)
  extends BinaryExpression with NullIntolerant {

  protected lazy val ordering: Ordering[Any] = TypeUtils.getInterpretedOrdering(left.dataType)

  protected override def nullSafeEval(left: Any, right: Any): Any = {
    EqualToTest.inc()
    ordering.equiv(left, right)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, (c1, c2) => s"""
    ${ctx.genEqual(left.dataType, c1, c2)};com.sparkutils.qualityTests.EqualToTest.inc()
    """)

  protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): EqualToTest =
    copy(left = newLeft, right = newRight)

  override def dataType: DataType = BooleanType
}