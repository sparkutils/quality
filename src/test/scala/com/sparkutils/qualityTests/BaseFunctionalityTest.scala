package com.sparkutils.qualityTests

import com.sparkutils.quality
import com.sparkutils.quality._
import functions._
import com.sparkutils.quality.impl.util.{Arrays, PrintCode, UpdateFields}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.{Column, DataFrame, Encoder, SaveMode}
import org.junit.Test
import org.scalatest.FunSuite

import java.util.UUID
import scala.language.postfixOps

class BaseFunctionalityTest extends FunSuite with RowTools with TestUtils {

  @Test
  def flattenResultsTest: Unit = evalCodeGensNoResolve {
    val rules = genRules(27, 27)
    val rulecount = rules.ruleSets.map( s => s.rules.size).sum

    val toWrite = 1 // writeRows

    val df = taddDataQuality(dataFrameLong(toWrite, 27, ruleSuiteResultType, null), rules)

    val exploded = df.select(expr("*"), expr("explode(flattenResults(DataQuality))").as("struct")).select("struct.*")
    val exploded2 = df.select(expr("*"), explode(flatten_results(col("DataQuality"))).as("struct")).select("struct.*")
    assert(exploded.union(exploded2).distinct().count == exploded.distinct().count)

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
  @Test
  def verifyResultExprDSL: Unit = evalCodeGens {
    assert(sparkSession.range(1).selectExpr("passed() p", "soft_failed() s", "disabled_rule() d", "failed() f")
      .select(expr("*"), passed as "p1", soft_failed as "s1", disabled_rule as "d1", failed as "f1")
      .filter("p1 = p and s1 = s and d1 = d and f1 = f").count == 1)
  }

  @Test
  def longPairEqual: Unit = evalCodeGens {
    import sparkSession.implicits._
    val (seq, ceq) = sparkSession.range(1).selectExpr("120 a_lower", "304 a_higher", "120 b_lower", "304 b_higher").
      select(expr("long_pair_equal('a','b') seq"), long_pair_equal("a","b") as "ceq").as[(Boolean, Boolean)].head

    assert(ceq)
    assert(seq)
  }

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
      selectExpr("*", "rngUUID(named_struct('lower', fparts.lower + id, 'higher', fparts.higher)) as f"," longPair(`id` + 0L, `id` + 1L) as rowid", "unpack(d) as g", "probability(1000) as prob",
        "as_uuid(fparts.lower + id, fparts.higher) as asUUIDExpr"
      ).select(expr("*"), unpack(col("d")) as "g2", as_uuid(expr("fparts.lower + id"), expr("fparts.higher")).as("asUUIDCol"))

    import sparkSession.implicits._
    val unpackCheck = df.selectExpr("g", "g2").as[((Int, Int), (Int, Int))].head
    assert(unpackCheck._1 == unpackCheck._2)

    df.drop("g2").write.mode(SaveMode.Overwrite).parquet(outputDir + "/simpleExprs")
    val re = sparkSession.read.parquet(outputDir + "/simpleExprs")
    val res = re.as[SimpleRes].orderBy(col("id").asc).head()
    assert(res match {
      case SimpleRes(1, FailedInt, PassedInt, SoftFailedInt, Packed, SoftFailedInt, DisabledRuleInt, ModifiedString, _, TestId, id, 0.01, ModifiedString, ModifiedString) => true
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
    not3_3 {
      // 3_3 has two keys with the same name, one boolean the other string so a CCE occurs when accessing the boolean thinking it's the string
      doTypeCheck(probability(lit(1.02)), tests :+ "however, 1.02 is of double type") // double as the lit input is a double
    }
  }

  def doTypeCheck(eval: String, containsTests: Seq[String]) : Unit =
    doTypeCheck(expr(eval), containsTests)

  def doTypeCheck(expr: Column, containsTests: Seq[String]) : Unit = evalCodeGens {
    val df = dataFrameLong(writeRows, 27, ruleSuiteResultType, null)

    var failed = false
    try {
      val exploded = df.select(expr)
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
              // 2/3 uses d on the end
              msg.contains(test.replaceAll("1.02", "1.02d")) ||
              // 3.4 uses the expression not the name
              msg.contains(test.replaceAll("overallResult","flattenresultsexpression(1)")) ||
              // 3.4 rc4 changes type syntax
              msg.contains(test.replaceAll("requires","requires the")) ||
              msg.contains(test.replaceAll("however,","however").replaceAll("type","").
                replaceAll("is of","has the type").trim())
            )
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
    import com.sparkutils.quality.implicits._
    import frameless._

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

  @Test
  def testComparableResults: Unit = evalCodeGensNoResolve {
    val rules = genRules(27, 27)
    val rulecount = rules.ruleSets.map( s => s.rules.size).sum

    val toWrite = 1 // writeRows

    val df = taddDataQuality(dataFrameLong(toWrite, 27, ruleSuiteResultType, null), rules).cache()
    val df2 = taddDataQuality(dataFrameLong(toWrite, 27, ruleSuiteResultType, null), rules).cache()

    // should fail
    try {
      (df union df2 distinct).show
      fail("Is assumed to fail as spark doesn't order maps")
    } catch {
      case t: Throwable =>
        assert(t.getMessage.toLowerCase.contains("map type"))
    }

    // can't resolve DataQuality here, manages quite nicely on it's own
    val comparable = df.selectExpr("*", "comparableMaps(DataQuality) compDQ").drop("DataQuality")
    val comparable2 = df2.select(expr("*"), comparable_maps(col("DataQuality")).as("compDQ")).drop("DataQuality")

    //comparable.show
    val unioned = comparable union comparable2 distinct

    unioned.show
    assert(unioned.count == df.count)
  }

  @Test
  def testComparableResultsDifferentKeysAndMapValue: Unit = evalCodeGensNoResolve {
    import sparkSession.implicits._

    def doCheck[T: Encoder](seq: Seq[T], thereCanBeOnlyOne: Boolean = false): Unit = {
      val df = seq.toDS()
      val df2 = seq.toDS()

      // should fail
      try {
        (df union df2 distinct).show
        fail("Is assumed to fail as spark doesn't order maps")
      } catch {
        case t: Throwable =>
          assert(t.getMessage.toLowerCase.contains("map type"))
      }

      // can't resolve DataQuality here, manages quite nicely on it's own
      val comparable = df.selectExpr("comparableMaps(value) v")
      val comparable2 = df2.select(comparable_maps(col("value")).as("v"))

      //comparable.show
      val unioned = comparable union comparable2 distinct

      unioned.show
      if (thereCanBeOnlyOne)
        assert(unioned.count == 1)
      else
        assert(unioned.count == df.count)

      unioned.sort("v").show
    }

    // arrays
    doCheck(Seq(Holder(Map(Seq(1,2) -> 1, Seq(2,1) -> 2 )), Holder(Map(Seq(2,1) -> 1, Seq(3,1) -> 1 ))))
    doCheck(Seq(Holder(Map(Seq(1,2) -> 1, Seq(2,1) -> 1 )), Holder(Map(Seq(2,1) -> 1, Seq(1,2) -> 1 ))), true)
    doCheck(Seq(Holder(Map(Seq(1,2) -> 1, Seq(2) -> 2 )), Holder(Map(Seq(2) -> 1, Seq(3,1) -> 1 ))))
    // maps
    doCheck(Seq(Holder(Map(Map(1 -> 2, 2 -> 2) -> 1, Map(3 -> 2, 4 -> 2) -> 1)), Holder(Map(Map(2 -> 1, 1 -> 2) -> 1)))) // 2 -> 2 is different
    doCheck(Seq(Holder(Map(Map(1 -> 2, 2 -> 2) -> 1)), Holder(Map(Map(2 -> 1, 1 -> 2) -> 1)))) // 2 -> 2 is different
    doCheck(Seq(Holder(Map(Map(1 -> 2, 2 -> 1) -> 1)), Holder(Map(Map(2 -> 1, 1 -> 2) -> 1))), true)

    // map value
    doCheck(Seq(Holder(Map(1 -> Map(1 -> 2, 2 -> 2))), Holder(Map(1 -> Map(2 -> 1, 1 -> 2))))) // 2 -> 2 is different
    doCheck(Seq(Holder(Map(1 -> Map(1 -> 2, 2 -> 1))), Holder(Map(1 -> Map(2 -> 1, 1 -> 2)))), true)
    // structs
    doCheck(Seq(Holder(Map(TestIdLeft(1, 2) -> 1)), Holder(Map(TestIdLeft(2, 1) -> 1))))
    doCheck(Seq(Holder(Map(TestIdLeft(1, 2) -> 1, TestIdLeft(2, 2) -> 1)), Holder(Map(TestIdLeft(2, 1) -> 1, TestIdLeft(3, 2) -> 1))))
    doCheck(Seq(Holder(Map(TestIdLeft(1, 2) -> 1)), Holder(Map(TestIdLeft(1, 2) -> 1))), true)
    // struct with map
    val map = Map(1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4)

    doCheck(Seq(Holder(NestedStruct(1, Map(12 -> MapArray(Seq(map))))), Holder(NestedStruct(1, Map(1 -> MapArray(Seq(map)))))))
    doCheck(Seq(Holder(NestedStruct(1, Map(1 -> MapArray(Seq(map))))), Holder(NestedStruct(1, Map(1 -> MapArray(Seq(map)))))), true)
  }

  @Test
  def testCompareWithArrays: Unit = evalCodeGensNoResolve {
    // testComparableResult does a test against the DQ results so hits nested maps and structs,
    // but there aren't arrays there so that code isn't tested

    val map = Map(1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4)

    val ma1 = MapArray(Seq(map))
    val ma2 = MapArray(Seq(map))
    import sparkSession.implicits._
    val ds1 = Seq(ma1, ma2).toDS()
    val ds2 = Seq(ma1, ma2).toDS()

    // should fail
    try {
      (ds1 union ds2 distinct).show
      fail("Is assumed to fail as spark doesn't order maps")
    } catch {
      case t: Throwable =>
        assert(t.getMessage.toLowerCase.contains("map type"))
    }

    // can't resolve DataQuality here, manages quite nicely on it's own
    val comparable = ds1.toDF.selectExpr("comparableMaps(seq) comp").drop("seq")
    val comparable2 = ds2.toDF.selectExpr("comparableMaps(seq) comp").drop("seq")

    //comparable.show
    val unioned = comparable union comparable2 distinct

    unioned.show
    assert(unioned.count == 1) // because all 4 are identical
  }

  @Test
  def testCompareWithArraysOrderingAndReverse: Unit = evalCodeGensNoResolve {
    // verifies it works in a sort

    val map = Map(1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4)
    val maps = (0 to 4).map(i => map.mapValues(_ * i))

    import sparkSession.implicits._
    val ds = maps.reverse.map(m => MapArray(Seq(m))).toDS()
    ds.show

    // should fail
    try {
      ds.sort("seq").show
      fail("Is assumed to fail as spark doesn't order maps")
    } catch {
      case t: Throwable =>
        assert(t.getMessage.contains("data type mismatch"))
    }

    // can't resolve DataQuality here, manages quite nicely on it's own
    val comparable = ds.toDF.selectExpr("comparableMaps(seq) seq")

    val sorted = comparable.sort("seq").selectExpr("reverseComparableMaps(seq) seq").as[MapArray]
    sorted.show
    sorted.collect().zipWithIndex.foreach{
      case (map,index) =>
        assert(map.seq(0) == maps(index)) // because all 4 are identical
    }

    val sorted2 = comparable.sort("seq").select(reverse_comparable_maps(col("seq")).as("seq")).as[MapArray]
    sorted2.collect().zipWithIndex.foreach{
      case (map,index) =>
        assert(map.seq(0) == maps(index)) // because all 4 are identical
    }
  }

  @Test
  def testCompareWithStructsReverseAndNested: Unit = evalCodeGensNoResolve {

    val map = Map(1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4)
    val maps = (0 to 4).map(i => map.mapValues(_ * i))

    import sparkSession.implicits._
    val ds = maps.reverse.map(m => NestedMapStruct(NestedStruct(m.head._1, Map( m.head._1 -> MapArray(Seq(m)))))).toDS()
    ds.show

    // should fail
    try {
      ds.sort("nested").show
      fail("Is assumed to fail as spark doesn't order maps")
    } catch {
      case t: Throwable =>
        assert(t.getMessage.contains("data type mismatch"))
    }

    // can't resolve DataQuality here, manages quite nicely on it's own
    val comparable = ds.toDF.selectExpr("comparableMaps(nested) seq")

    val sorted = comparable.sort("seq").selectExpr("reverseComparableMaps(seq) nested").as[NestedMapStruct]
    sorted.show
    sorted.collect().zipWithIndex.foreach {
      case (struct, index) =>
        assert(struct.nested.nested.head._2.seq(0) == maps(index)) // because all 4 are identical

    }

  }

  @Test
  def mapArrays(): Unit = {
    val ar = ArrayData.toArrayData(Seq(0,1,2,3,4)) // Force GenericArrayData instead of UnsafeArrayData
    val nar = Arrays.mapArray(ar, IntegerType, _.asInstanceOf[Integer] + 1)
    assert((0 until 5).forall(i => nar(i) == i + 1))

    // verify with simple toArray
    val nar2 = Arrays.toArray(ar, IntegerType)
    assert((0 until 5).forall(i => nar2(i) == i))
  }

  @Test
  def scalarSubqueryAsTrigger(): Unit = evalCodeGensNoResolve {
    v3_4_and_above {
      // assert that using a join to test with is fine even when nested
      import sparkSession.implicits._
      val seq = Seq(0, 1, 2, 3, 4)
      val df = seq.toDF("i") // Force GenericArrayData instead of UnsafeArrayData
      val tableName = "the_I_s_Have_It"
      df.createOrReplaceTempView(tableName)

      def sub(comp: String = "> 2", tableSuffix: String = "") = s"exists(select 0 from $tableName i_s$tableSuffix where i_s$tableSuffix.i $comp)"

      val baseline = sparkSession.sql(s"select struct(${sub()}).col1 is not null")
      assert(!baseline.isEmpty)
      assert(baseline.as[Boolean].head())

      val rs = RuleSuite(Id(1, 1), Seq(
        RuleSet(Id(50, 1), Seq(
          Rule(Id(100, 1), ExpressionRule(sub("> main.i and i_s.i < 3"))),
          Rule(Id(101, 1), ExpressionRule(sub("> main.i")))
        ))
      ))
      val testDF = seq.toDF("i").as("main")
      testDF.collect()
      val resdf = addDataQuality(testDF, rs)
      try {
        val res = resdf.selectExpr("DataQuality.overallResult").as[Int].collect()
        assert(res.filterNot(_ == PassedInt).length == 3) // 1 with just 101 above, 3 fail with 100 as well
      } catch {
        case t: Throwable =>
          throw t
      }
    }
  }

  @Test
  def functionParameterSizes(): Unit = try {
    sparkSession.sql("select inc(1,34,3243,666)")
    fail("Should have thrown")
  } catch {
    case QualityException(m,_) if m.contains("counts are 1, 0, 2") => ()
  }

  @Test
  def testRuleResult(): Unit = evalCodeGensNoResolve {
    val rules = genRules(27, 27)

    val toWrite = 1400 // writeRows

    val df = taddDataQuality(dataFrameLong(toWrite, 27, ruleSuiteResultType, null), rules)

    doTheRuleResultTest(df, toWrite)
  }

  @Test
  def testRuleResultDetails(): Unit = evalCodeGensNoResolve {
    val rules = genRules(27, 27)

    val toWrite = 1400 // writeRows

    val df = taddDataQuality(dataFrameLong(toWrite, 27, ruleSuiteResultType, null), rules)

    doTheRuleResultTest(df.selectExpr("rule_Suite_Result_Details(DataQuality) DataQuality"), toWrite)
    doTheRuleResultTest(df.select(rule_suite_result_details(col("DataQuality")) as "DataQuality"), toWrite)
  }

  def doTheRuleResultTest(df: DataFrame, toWrite: Int): Unit = {
    import sparkSession.implicits._

    // using a lambda to ensure it works to reduce code duplication
    val l = LambdaFunction("dq_rule_result", "(a, b) -> rule_result(DataQuality, pack_ints(1,1), a, b)", Id(0,0))
    registerLambdaFunctions(Seq(l))

    // count_if not on 2.4
    def count_if(cond: String) = s"aggExpr($cond, inc(), returnSum())"

    val theRule = df.selectExpr(
      count_if("dq_rule_result(pack_ints(52,1), pack_ints(100,1)) == passed()"),
      count_if("rule_result(DataQuality, pack_ints(100, 1), pack_ints(52,1), pack_ints(100,1)) == passed()"),
      count_if("dq_rule_result(pack_ints(152,1), pack_ints(100,1)) == passed()"),
      count_if("dq_rule_result(pack_ints(52,1), pack_ints(10000,1)) == passed()"),
      count_if("dq_rule_result(pack_ints(52,1), null) == passed()")
    )
    val (shouldBeHalfPassed, shouldNotBeFoundSuite, shouldNotBeFoundSet, shouldNotBeFoundRule, hasNulls) = theRule.as[(Long, Long, Long, Long, Long)].head()
    assert(shouldBeHalfPassed == ((toWrite / 2) + 1))

    assert(shouldNotBeFoundSuite == 0)
    assert(shouldNotBeFoundSet == 0)
    assert(shouldNotBeFoundRule == 0)
    assert(hasNulls == 0)
  }

  @Test
  def testExpressionsWithAggregate(): Unit = evalCodeGensNoResolve {
    val rowrs = RuleSuite(Id(11, 2), Seq(RuleSet(Id(21, 1), Seq(
      Rule(Id(40, 3), ExpressionRule("iseven(id)"))
    ))), lambdaFunctions = Seq(LambdaFunction("iseven", "p -> p % 2 = 0", Id(1020, 2))))

    val rs = RuleSuite(Id(10, 2), Seq(RuleSet(Id(20, 1), Seq(
      Rule(Id(30, 3), ExpressionRule("sum(id)")),
      Rule(Id(31, 3), ExpressionRule("aggExpr(rule_result(DataQuality, pack_ints(11,2), pack_ints(21,1), pack_ints(40,3)) == passed(), inc(), returnSum())"))
    ))))

    import quality.implicits._

    val processed = taddDataQuality(sparkSession.range(1000).toDF, rowrs).select(expressionRunner(rs))

    val res = processed.selectExpr("expressionResults.*").as[GeneralExpressionsResult].head()
    assert(res == GeneralExpressionsResult(Id(10, 2), Map(Id(20, 1) -> Map(
      Id(30, 3) -> GeneralExpressionResult("499500", "BIGINT"),
      Id(31, 3) -> GeneralExpressionResult("500", "BIGINT")
    ))))

    val gres =
      processed.selectExpr("rule_result(expressionResults, pack_ints(10,2), pack_ints(20,1), pack_ints(31,3)) rr")
        .selectExpr("rr.*").as[GeneralExpressionResult].head

    assert(gres == GeneralExpressionResult("500", "BIGINT"))

    val stripped = processed.selectExpr("strip_result_ddl(expressionResults) rr")
    val stripped2 = processed.select(strip_result_ddl(col("expressionResults")) as "rr")
    assert(stripped.select(comparable_maps(col("rr"))).union( stripped2.select(comparable_maps(col("rr"))) ).distinct.count == 1)

    val strippedRes = stripped.selectExpr("rr.*").as[GeneralExpressionsResultNoDDL].head()
    assert(strippedRes == GeneralExpressionsResultNoDDL(Id(10, 2), Map(Id(20, 1) -> Map(
      Id(30, 3) -> "499500",
      Id(31, 3) -> "500")
    )))

    val strippedGres = {
      import sparkSession.implicits._
      stripped.select(rule_result(col("rr"), pack_ints(10,2), pack_ints(20,1), pack_ints(Id(31,3))))
        .as[String].head
    }

    assert(strippedGres == "500")
  }

  @Test
  def updateFields(): Unit = evalCodeGens {
    import sparkSession.implicits._

    val og = sparkSession.range(1).selectExpr("named_struct('a', 1, 'b', named_struct('c', 4, 'd', named_struct('e', 'wot'))) s")
    def assertsbc(df: DataFrame, expected: Int, expectedString: String) = {
      assert(df.select("s.b.c").as[Int].head() == expected)
      assert(df.select("s.b.d.e").as[String].head() == expectedString)
    }

    assertsbc(og, 4, "wot")

//    val updated = og.select(update_field(col("s"), ("b", update_field(col("s.b"), ("c", lit(40))))) as "s")
    val updated = og.select(update_field(col("s"), ("b.c", lit(40)), ("b.d.e", lit("mate"))) as "s")
    //val updated = og.select(new Column(UpdateFields.apply(UpdateFields.apply(col("s").expr, "b.c", lit(40).expr), "b.d.e", lit("mate").expr)) as "s")
    assertsbc(updated, 40, "mate")

    // b.d.e doesn't work as it'd force d to be empty.
    val schema = updated.selectExpr("drop_field(s, 'b.d')").schema
    val b = schema.fields(0).dataType.asInstanceOf[StructType].fields(1).dataType.asInstanceOf[StructType].fields
    assert(b.length == 1)
    assert(b.apply(0).name == "c")
  }

  @Test
  def checkMinimumLengthWorks(): Unit =
    try {
      sparkSession.range(1).selectExpr("hash_with()").show
      fail("should have thrown")
    } catch {
      case t: Throwable if t.getMessage.contains("A minimum of 2 parameters is required") =>
        ()
    }
}

object Holder {
  var res: String = ""
}

case class Holder[T](value: T)

case class MapArray(seq: Seq[Map[Int, Int]])

case class NestedStruct(field: Int, nested: Map[Int, MapArray])
case class NestedMapStruct( nested: NestedStruct)

case class TestIdLeft(left_lower: Long, left_higher: Long)
case class TestIdRight(right_lower: Long, right_higher: Long)

case class SimpleRes(id: Int, a: Int, b: Int, c: Int, d: Long, e: Int, dis: Int, f: String, throwaway: String, rowId: RowId, g: Id, prob: Double, asUUIDExpr: String, asUUIDCol: String)
