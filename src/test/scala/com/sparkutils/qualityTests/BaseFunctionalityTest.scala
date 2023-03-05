package com.sparkutils.qualityTests

import java.util.UUID
import com.sparkutils.quality._
import com.sparkutils.quality.utils.PrintCode
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.junit.Test
import org.scalatest.FunSuite

class BaseFunctionalityTest extends FunSuite with RowTools with TestUtils {

  @Test
  //@elidable(1) // does not work on 3.2 need to elide - possibly due to https://issues.apache.org/jira/browse/SPARK-37392 and Databricks' ES-213117
  def flattenResultsTest: Unit = evalCodeGensNoResolve {
    val rules = genRules(27, 27)
    val rulecount = rules.ruleSets.map( s => s.rules.size).sum

    val toWrite = 1 // writeRows

    val df = taddDataQuality(dataFrameLong(toWrite, 27, ruleSuiteResultType, null), rules)

    // fails on 3.2 when the deserializer is not evaluated by spark before use
    //val exploded = df.select(expr("*"), expr("explode(flattenResults(DataQuality))").as("struct")).select("*","struct.*")
    val exploded = df.select(expr("*"), expr("explode(flattenResults(DataQuality))").as("struct")).select("struct.*")
    // works on 3.2
//    val exploded = df.select(expr("*"), expr("flattenResults(DataQuality)").as("flattened")).select(expr("explode(flattened)").as("struct")).select("struct.*")

    //println(exploded.queryExecution)
    //exploded.show(84)

    // verify fields are there
    assert(!exploded.select("ruleSuiteId", "ruleSuiteVersion", "ruleSuiteResult", "ruleSetResult", "ruleSetId", "ruleSetVersion", "ruleId", "ruleVersion", "ruleResult").isEmpty, "Flattened fields should be present")

    // it's sufficient to run the results for this test, if it can't be encoded it will throw on count
    assert(rulecount * (toWrite + 1) == exploded.count(), "exploded count size was unexpected")
  }

  @Test
  def flattenResultsWithMissingTest: Unit = evalCodeGensNoResolve {
    val rules = genRules(27, 27)
    val rulecount = rules.ruleSets.map( s => s.rules.size).sum

    val toWrite = 1 // writeRows

    val dfl = dataFrameLong(toWrite, 27, ruleSuiteResultType, null)
    // make sure it's not messing up parsing in the basic rule only case
    val missing = processIfAttributeMissing(rules, ruleSuiteResultType)
    // in this case we should validate only after any coalesceIfAttributesMissing[Disable] is removed
    val (errors, _) = validate(dfl, missing)

    val df = taddDataQuality(dfl, missing)

    // fails on 3.2 when the deserializer is not evaluated by spark before use
    val exploded = df.select(expr("*"), expr("explode(flattenResults(DataQuality))").as("struct")).select("struct.*")//.select("*","struct.*")
    // works on 3.2
    //    val exploded = df.select(expr("*"), expr("flattenResults(DataQuality)").as("flattened")).select(expr("explode(flattened)").as("struct")).select("struct.*")

    //println(exploded.queryExecution)
    //exploded.show(84)

    // verify fields are there
    assert(!exploded.select("ruleSuiteId", "ruleSuiteVersion", "ruleSuiteResult", "ruleSetResult", "ruleSetId", "ruleSetVersion", "ruleId", "ruleVersion", "ruleResult").isEmpty, "Flattened fields should be present")

    // it's sufficient to run the results for this test, if it can't be encoded it will throw on count
    assert(rulecount * (toWrite + 1) == exploded.count(), "exploded count size was unexpected")
  }
/*
  @Test // 3.2 is random when the deserializer is not pushed through the planner / query execution phases - which is nice
  def loadsOfFlattens: Unit = loadsOf(flattenResultsTest)

  def loadsOf(thunk: => Unit, runs: Int = 300): Unit = {
    var passed = 0
    for{ i <- 0 until runs }{
      try {
        thunk
        passed += 1
      } catch {
        case e: org.scalatest.exceptions.TestFailedException => println("failed "+e.getMessage())
        case t: Throwable => println("failed unexpectedly "+t.getMessage())
      }
    }
    assert(passed == runs, "Should have passed all of them, nothing has changed in between runs")
  }*/
/* disabled as it's tested above with the 3.2 workaround
  @Test // 3.2 is random - which is nice
  def loadsOfFlattensReduced: Unit = loadsOf(flattenResultsTestReduced, 30) // 300 takes too long

  @Test
  def flattenResultsTestReduced: Unit = evalCodeGensNoResolve {
    val rules = genRules(27, 27)
    val rulecount = rules.ruleSets.map( s => s.rules.size).sum

    val df = taddDataQuality(dataFrameLong(writeRows, 27, ruleSuiteResultType, null), rules)

    val exploded = df.selectExpr("size(flattenResults(DataQuality)) as toCount")

    //println(exploded.queryExecution)
    //exploded.show(84)

    import sparkSession.implicits._
    val res = exploded.selectExpr("sum(toCount)").as[Long].head

    // it's sufficient to run the results for this test, if it can't be encoded it will throw on count
    assert(rulecount * (writeRows + 1) == res, "exploded count size was unexpected")
  }
*/
  @Test // probabilityIn is covered by bloomtests, flatten by explodeResultsTest
  def verifySimpleExprs: Unit = evalCodeGens {

//    val log = LogManager.getLogger("Dummy")
//    log.error("OIOIOIO")


    val orig = UUID.randomUUID()
    val uuid = orig.toString
    val ModifiedString = new UUID(orig.getMostSignificantBits, orig.getLeastSignificantBits + 1).toString

    val TestId = RowId(1, 2)
    val id = Id(1024, 9084)
    val Packed = packId(id)

    val starter = sparkSession.range(1,500).selectExpr("cast(id as int) as id").selectExpr("*","failed() * id as a", "passed() * id as b", "softFailed() * 1 as c", "disabledRule() as dis")
    val df = starter.selectExpr("*", "packInts(1024 * id, 9084 * id) as d", "softFail( (1 * id) > 2 ) as e", s"longPairFromUUID('$uuid') as fparts").
      selectExpr("*", "rngUUID(longPairFromUUID(uuid())) throwaway").
      selectExpr("*", "rngUUID(named_struct('lower', fparts.lower + id, 'higher', fparts.higher)) as f"," longPair(`id` + 0L, `id` + 1L) as rowid", "unpack(d) as g", "probability(1000) as prob")
    df.write.mode(SaveMode.Overwrite).parquet(outputDir + "/simpleExprs")
    val re = sparkSession.read.parquet(outputDir + "/simpleExprs")
    import sparkSession.implicits._
    val res = re.as[SimpleRes].orderBy(col("id").asc).head()
    assert(res match {
      case SimpleRes(1, FailedInt, PassedInt, SoftFailedInt, Packed, SoftFailedInt, DisabledRuleInt, ModifiedString, _, TestId, id, 0.01) => true
      case _ => false
    })
  }

  @Test
  def typeCheckFlatten: Unit = evalCodeGens {
    doTypeCheck("flattenResults(1)", Seq("cannot resolve", "overallResult","however, 1 is of int type"))
  }

  @Test
  def typeCheckPackInts: Unit = evalCodeGens {
    val tests = Seq("cannot resolve", "requires int type","however, a is of string type")
    doTypeCheck("packInts(1, 'a')", tests :+ "parameter 2") // argument for <3.4
    doTypeCheck("packInts('a', 1)", tests :+ "parameter 1")
  }

  @Test
  def typeCheckProbability: Unit = evalCodeGens {
    val tests = Seq("cannot resolve", "requires (int or bigint) type")
    doTypeCheck("probability('a')", tests :+ "however, a is of string type")
    doTypeCheck("probability(1.02)", tests :+ "however, 1.02 is of decimal(3,2) type") // NB will be double in Spark 3.0
  }

  def doTypeCheck(eval: String, containsTests: Seq[String]) : Unit = evalCodeGens {
    val df = dataFrameLong(writeRows, 27, ruleSuiteResultType, null)

    var failed = false
    try {
      val exploded = df.select(expr(eval))
      exploded.count
      failed = true
    } catch {
      case t: Throwable =>
        // 3.4 upper cases types, has "" for values (and wraps types with them) without '' for inner strings, swaps argument for parameter and
        // drops the Big Decimal BD type suffix
        val msg = t.getMessage.toLowerCase.replaceAll("argument","parameter").replaceAll("'", "").
          replaceAll("\"","").replaceAll("bd","")
        containsTests.foreach{
          test =>
            assert(msg.contains(test) ||
              // 3.4 uses the expression not the name
              msg.contains(test.replaceAll("overallResult","flattenresultsexpression(1)")))
        }
    }
    if (failed) {
      fail("Should have thrown")
    }
  }

  @Test
  def positiveProbResults: Unit = doSimpleOverallEval(simplePassedProbabilityRule, Passed)

  @Test
  def negativeProbResults: Unit = doSimpleOverallEval(simpleFailedProbabilityRule, Failed)

  @Test
  def positiveProbResultsOverridden: Unit = doSimpleOverallEval(simplePassedProbabilityRule.withProbablePass(0.95), Failed)

  @Test
  def negativeProbResultsOverridden: Unit = doSimpleOverallEval(simpleFailedProbabilityRule.withProbablePass(0.5), Passed)

  val simpleDisabledRule = RuleSuite(Id(1,1), Seq(
    RuleSet(Id(50, 1), Seq(
      Rule(Id(100, 1), ExpressionRule("disabledRule()"))
    ))
  ))

  @Test
  def disabledOverallShouldBePassed: Unit = doSimpleOverallEval(simpleDisabledRule, Passed)

  val simplePrimitiveButNotLiteralRule = RuleSuite(Id(1,1), Seq(
    RuleSet(Id(50, 1), Seq(
      Rule(Id(100, 1), ExpressionRule("-1657899192881000L")),
      Rule(Id(101, 1), ExpressionRule("-4")), // repeat of simpleDisabledRule really
      Rule(Id(102, 1), ExpressionRule("false")),
      Rule(Id(102, 1), ExpressionRule("cast(-1.32 as float)")),
      Rule(Id(102, 1), ExpressionRule("cast(-2 as short)")),
      Rule(Id(102, 1), ExpressionRule("cast(-2 as byte)")),
      Rule(Id(102, 1), ExpressionRule("cast(-1.32 as double)"))
    ))
  ))

  @Test
  def oddBoxingIssueShouldRun: Unit = doSimpleOverallEval(simplePrimitiveButNotLiteralRule, null) // we don't care about the result

  def doSimpleOverallEval(ruleSuite: RuleSuite, expected: RuleResult): Unit =  evalCodeGens {
    import frameless._

    import com.sparkutils.quality.implicits._

    val df = {
      import sparkSession.implicits._ // can't have it use these defaults or it fails on versionedid and friends
      sparkSession.createDataset(Seq(1)).toDF()
    }
    val res = df.transform(taddDataQualityF(ruleSuite)).select("DataQuality.*").as[RuleSuiteResult].collect()
    if (expected ne null) {
      assert(res.head.overallResult == expected)
    }

    implicit val enc = TypedExpressionEncoder[(RuleResult, RuleSuiteResultDetails)]

    val res2 = df.transform(taddOverallResultsAndDetailsF(ruleSuite)).select("DQ_overallResult", "DQ_Details").as[(RuleResult, RuleSuiteResultDetails)].collect()
    assert(res2.head._1 == res.head.overallResult)
    assert(res2.head._2 == res.head.details)
  }

  @Test
  def testPrintExpr(): Unit =
    doTestPrint("Expression toStr is ->", "my message is", "my message is", "plus(1, 1, lambda", "printExpr")

  // 2.4 doesn't support forceInterpreted so we can't test that it _doesn't_ compile, databricks is cluster based so we'll not be able to capture it without dumping to files
  @Test
  def testPrintCode(): Unit = not_Databricks{ not2_4 {
    // using eval we shouldn't get output
    forceInterpreted {
      doTestPrint(null, "my message is", null, null, "printCode")
    }

    // should generate output for code gen
    forceCodeGen {
      doTestPrint(PrintCode(lit("").expr).msg, "my message is", "my message is", "private int FunN_0(InternalRow i)", "printCode")
    }
  }}

  def doTestPrint(default: String, custom: String, customTest: String, addTest: String, expr: String): Unit = {
    import com.sparkutils.quality._
    import sparkSession.implicits._
    val plus = LambdaFunction("plus", "(a, b) -> a + b", Id(3,2)) // force compile with codegen
    registerLambdaFunctions(Seq(plus))
    registerQualityFunctions(writer = {Holder.res = _})

    import Holder.res
    assert(2 == sparkSession.sql(s"select $expr(plus(1, 1)) as res").as[Long].head)

    def assertAdd() = if (addTest ne null) {
      assert(res.contains(addTest))
    }

    if (default ne null)
      assert(res.indexOf(default) == 0)
    else
      assert(res.isEmpty)
    assertAdd()

    assert(2 == sparkSession.sql(s"select $expr('$custom', plus(1, 1)) as res").as[Long].head)

    if (customTest ne null)
      assert(res.indexOf(customTest) == 0)
    else
      assert(res.isEmpty)

    assertAdd()
  }

}

object Holder {
  var res: String = ""
}

case class TestIdLeft(left_lower: Long, left_higher: Long)
case class TestIdRight(right_lower: Long, right_higher: Long)

case class SimpleRes(id: Int, a: Int, b: Int, c: Int, d: Long, e: Int, dis: Int, f: String, throwaway: String, rowId: RowId, g: Id, prob: Double)
