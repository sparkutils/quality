package com.sparkutils.qualityTests

import com.sparkutils.quality._
import org.apache.spark.sql.DataFrame
import types._
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{DataType, LongType}
import org.junit.Ignore
// 3.4 drops this import org.codehaus.janino.InternalCompilerException
import org.junit.Assert.fail
import org.junit.Test

/**
 * Attempts to force wholestagecodegen with enough rules to force the 64k hit, then prove the codegen options resolve them.
 * Only triggered by forcing resolveWith, spark decides, probably correctly, to eval otherwise.
 *
 * Alas it adds another minute to the testing, but worth it.  It will add more when engine is codegen'd ...
 */
//@Ignore
class CodeGenTest extends RowTools with TestUtils {

  def readGen[T](func: (Int, Int, DataFrame) => DataFrame, dataType: DataType, startValue: T)  = {

    val df = dataFrameLong(40, 1600, dataType, startValue)
    val ndf = func(10, 1600, df)

    import frameless._

    import com.sparkutils.quality.implicits._

    implicit val enc = TypedExpressionEncoder[(RuleResult, RuleSuiteResultDetails)]

    val res2 =
      ndf.select("DQ_overallResult", "DQ_Details").as[(RuleResult, RuleSuiteResultDetails)].collect()

    res2
  }

  // seperate impl as we need genRules and overridden variables per func / group
  def doRunnerGen(variablesPerFunc: Int, variableFuncGroup: Int): Unit = {
    val res = readGen[RuleSuiteResult](
      (rules: Int, cols: Int, df: DataFrame) => {
        val temporaryDQname: String = "DQ_TEMP_Quality"

        df.transform { ndf =>
          val rr = ruleRunner(genRules(rules, cols),
            compileEvals = false,
            variablesPerFunc = variablesPerFunc, variableFuncGroup = variableFuncGroup,
            resolveWith = Some(df)) // also needed to force code gen
          ndf.select(expr("*"), rr.as(temporaryDQname)).
            selectExpr("*", s"$temporaryDQname.overallResult as DQ_overallResult",
              s"ruleSuiteResultDetails($temporaryDQname) as DQ_Details").drop(temporaryDQname)
        }.asInstanceOf[DataFrame]
      }, ruleSuiteResultType, null)
    //res foreach println
  }

  def shouldAssert64kb(f: => Unit): Unit ={
    var finished = false
    try {
      f
      finished = true
    } catch {
      case e: Throwable if anyCauseHas(e, {
        case ice: Exception /*InternalCompilerException*/ if ice.getMessage.contains("grows beyond 64 KB") => true
        case _ => false
      }) =>
        ()
      case t: Throwable => {
        val c = t.getCause
        fail("Should have throw 64k, instead got "+t.getMessage)
      }
    }
    if (finished) {
      fail("Did not throw an ICE for 64kb")
    }
  }

  /* GC's on 3.4, taking 2m locally, after 0.1.3.1-RC5 no longer happens with new compilation approach
  @Test
  def ruleRunnerTooMuchPerFunc: Unit = not3_4_or_above{ not_Cluster{ not2_4{ forceCodeGen {
    shouldAssert64kb{
      doRunnerGen(variablesPerFunc= 30000, variableFuncGroup = 12)
    }
  }}}}*/

  @Test
  def ruleRunnerDefault: Unit = not2_4{ forceCodeGen {
    doRunnerGen(variablesPerFunc = 40, variableFuncGroup = 20)
  }}

  def doEngineRunnerGen(variablesPerFunc: Int, variableFuncGroup: Int, set: Int = 10, rules: Int = 1600): Unit = {
    def genEngineRules(rules: Int, cols: Int) = {
      val ruleSuite = genRules(rules, cols)
      ruleSuite.copy( ruleSets = ruleSuite.ruleSets.map( ruleSet =>
        ruleSet.copy( rules =
          ruleSet.rules.map(rule =>
            rule.copy( runOnPassProcessor =
              RunOnPassProcessor(0,
                rule.id.copy(id = rule.id.id + 11111)
                , OutputExpression("`1` + `10`")) // bizarrely if you use ""+rule.id.id it seems to trick compilation into verify errors
            )
          )
      )))
    }

    def func(rules: Int, cols: Int, df: DataFrame) = {
      val temporaryDQname: String = "DQ_TEMP_Quality"

      df.transform { ndf =>
        val rr = ruleEngineRunner(genEngineRules(rules, cols),
          resultDataType = LongType,
          compileEvals = false,
          variablesPerFunc = variablesPerFunc, variableFuncGroup = variableFuncGroup,
          resolveWith = Some(df)) // also needed to force code gen
        ndf.select(expr("*"), rr.as(temporaryDQname)).
          selectExpr(s"$temporaryDQname.*").drop(temporaryDQname)
      }
    }

    val df = dataFrameLong(40, 1600, ruleSuiteResultType, null)
    //df.show
    val ndf = func(set, rules, df)

    import frameless._

    import com.sparkutils.quality.implicits._

    val res = ndf.as[RuleEngineResult[Long]].collect()

    //res
  }

  @Ignore // as of e149d590 (#71) no longer triggered on Spark4
  def ruleEngineRunnerTooMuchPerFunc: Unit = not_Cluster{ not2_4{ forceCodeGen {
    // as of eec8842 does not hit 64k on the server at 900
    shouldAssert64kb{
      doEngineRunnerGen(variablesPerFunc= 3000, variableFuncGroup = 12)
    }
  }}}

  @Test
  def ruleEngineRunnerDefault: Unit = not2_4{ forceCodeGen {
    doEngineRunnerGen(variablesPerFunc = 40, variableFuncGroup = 20)//, set = 2, rules = 4)
  }}

}
