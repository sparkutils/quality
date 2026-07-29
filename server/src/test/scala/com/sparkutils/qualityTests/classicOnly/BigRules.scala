package com.sparkutils.qualityTests.classicOnly

import com.sparkutils.quality.{groupProcessorPercentFilter, _}
import com.sparkutils.quality.impl.util.ExtraConfig.ConfigMapOps
import com.sparkutils.quality.impl.util.RuleSuiteGroupIOUtils
import com.sparkutils.quality.impl.TopLevelBooleanGrouper
import com.sparkutils.quality.impl.mapLookup.MapLookupFunctions
import com.sparkutils.qualityTests.classicOnly.BigRulesGen.{genRules1to1, genRulesMap, testFile}
import com.sparkutils.qualityTests.util.ClassicSharedTests
import com.sparkutils.testing.ConnectionType
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.scalatest.Matchers

import scala.concurrent.duration.Duration

object BigRulesGen {

  def testfileResource = getClass.getResourceAsStream("/20k_rule_suite.csv")

  // only available on 3.5
  val replace = org.apache.spark.sql.functions.udf((source: String, against: String, withWhat: String) =>
    source.replace(against, withWhat))

  val f_bucket_size = 40
  val fModExpr = s"if(f = '*', 0, hash(f) % $f_bucket_size)"

  def testFile(s: SparkSession, outputDir: String, extra: String = ""): String = {
    val tmp = outputDir + s"/20k_rule_suite$extra.csv"
    // use spark to "copy" the file so Fabric/Databricks can work with correct auth.
    val res = testfileResource
    var source: scala.io.Source = null
    try {
      source = scala.io.Source.fromInputStream(res)
      val itr = source.getLines()
      val values = itr.map(_.split(",").toSeq).toSeq
      import s.implicits._

      val header = values.head
      val df = values.drop(1).map(s => Tuple12(s(0), s(1), s(2), s(3), s(4), s(5), s(6), s(7), s(8), s(9), s(10), s(11))).
        toDF(header: _*)
      df.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv(tmp)
    } finally {
      source.close()
      IOUtils.closeQuietly(res)
    }
    tmp
  }

  def genRules1to1(s: SparkSession, outputDir: String, withF: Boolean = false) = {
    val d = s.read.option("header",true).csv(testFile(s,outputDir))
    val cols = d.columns.toSet -- Set("k","l", "id") -- (
      if (withF)
        Set("f")
      else
        Set.empty
    )
    def exprOf(name: String): String = s"if($name = '*', 'remove', '$name = \\'' || $name || '\\'')"
    val ruleGen = cols.toSeq.map(exprOf).mkString(" || ' and ' || ")
    val ruleDS = d.select(Seq(
      replace(
        replace(expr(ruleGen), lit("remove and "), lit("")),
        lit("and remove"), lit("")
      ).as("_1"),
      expr("'struct(\\'' ||  k || '\\',\\'' || l || '\\')'").
        as("_2"), expr("id").cast(IntegerType).as("_3")) ++ (
      if (withF)
        Seq(expr("f"), expr(fModExpr).as("f_mod"))
      else
        Seq.empty
    ) :_*)
    ruleDS
  }


  def genRulesMap(s: SparkSession, outputDir: String, withId: Boolean = false) = {
    import s.implicits._
    val d = s.read.option("header",true).csv(testFile(s,outputDir))
    val cols = (d.columns.toSet -- Set("k","l","id")).toSeq.sorted
    def exprOf(name: String): String = s"if($name = '*', 'remove', '$name')"
    val ruleGen = cols.map(exprOf).mkString(" || ' , ' || ")

    def filterOf(name: String): String = s"""if($name = '*', "$name = '*'", "$name != '*'")"""
    val filterGen = cols.map(filterOf).mkString(" || ' and ' || ")

    val ruleDS = d.select(
      Seq(
        concat(lit("struct("),
          replace(
            replace(expr(ruleGen), lit("remove , "), lit("")),
            lit(", remove"), lit("")
          ), lit(")")).as("trigger"),
        expr("'struct(k, l)'").
          as("output"),
        (lit(1000) -
          aggregate(array(cols.toSeq.map(name => expr(s"if($name = '*', 0, 1)")) :_*), lit(0), (a, b) => a + b)
          ).as("salience"),
        expr(filterGen).as("filter")
      ) ++ (
        if (withId)
          Seq(expr("id"))
        else
          Seq.empty
        ) : _*
    ).distinct()
    ruleDS
  }

}

trait BigRulesBase extends Matchers {

  def outputDir: String

  def rules(s: SparkSession, dataSet: Dataset[(String, String, Int)]) = {
    //.write.mode(SaveMode.Overwrite).option("header",true).csv(outputDir + "/rules.csv")
    val rules = dataSet.orderBy("_3").collect().map{
      case (trigger, output, id) =>

        Rule(Id(id, 1), ExpressionRule(trigger),
              RunOnPassProcessor(1000 + id, Id(1040 + id, 1), OutputExpression(output)))
    }
    RuleSuite(Id(1,0), Seq(
      RuleSet(Id(50, 1), rules
      )))
  }

  def doRuleTest(s: SparkSession, ruleSuite: RuleSuite, typ: String, resultDataType: Option[DataType] = Some(
    StructType(Seq(
      StructField("k_out", StringType),
      StructField("l_out", StringType)
    )
  )), topLevelRunner: (RuleSuite, Option[DataType], Map[String, String]) => Column =
      (rs, dt, op) => ruleEngineRunner(rs, dt, extraConfig = op), processor: DataFrame => DataFrame =
        _.select(expr("*"), expr("runner.result.*")), extraConfig: Map[String, String] = Map.empty): DataFrame = {

    var start = System.nanoTime()
    val d = s.read.option("header",true).csv(testFile(s,outputDir))
    val r = processor(d.select(expr("*"), topLevelRunner(ruleSuite, resultDataType, extraConfig).
      as("runner")/*, col("runner.result"), col("runner.salientRule")*/))
    var end = System.nanoTime()

    println(s"$typ - took ${Duration.fromNanos(end - start).toSeconds}s to do logical plan")
    start = System.nanoTime()
    r.write.format("noop").mode(SaveMode.Overwrite).save()
    end = System.nanoTime()
    val fullDump = Duration.fromNanos(end - start)
    println(s"$typ - took ${fullDump.toMinutes}m${fullDump.toSeconds % 60}s to do full noop write")

    /*
var start = System.nanoTime()
    val d = s.read.option("header",true).csv("server/src/test/resources/20k_rule_suite.csv")
    val r = d.select(expr("*"), ruleEngineRunner(ruleSuite, resultDataType = resultDataType).
      as("runner")).select(expr("*"), expr("runner.result.*"))
    var end = System.nanoTime()

    println(s"$typ - took ${Duration.fromNanos(end - start).toSeconds}s to do logical plan")
    *//*
    start = System.nanoTime()
    r.limit(1).write.format("noop").mode(SaveMode.Overwrite).save()
    end = System.nanoTime()
    val compilationEstimation = Duration.fromNanos(end - start)
    println(s"$typ - took ${compilationEstimation.toMinutes}m${compilationEstimation.toSeconds % 60}s to do a limit 1, closest to compile time")
*//*
    start = System.nanoTime()
    r.write.format("noop").mode(SaveMode.Overwrite).save()
    end = System.nanoTime()
    val fullDump = Duration.fromNanos(end - start)
    val processOf20kx20k = fullDump // - compilationEstimation
    println(s"$typ - took ${fullDump.toMinutes}m${fullDump.toSeconds % 60}s to do full noop write, of which" +
      s" ${processOf20kx20k.toMinutes}m${processOf20kx20k.toSeconds % 60}s in processing 20kx20k")
    r
*/
    r
  }

  def doTriggerGetValueShouldWork(s: SparkSession): Unit = {
    Map.empty[String, String].boolean(showSplitCompilationTime, false) shouldBe false

    Map(
      showSplitCompilationTime -> "true"
    ).boolean(showSplitCompilationTime, false) shouldBe true

    try {
      System.setProperty(showSplitCompilationTime, "true")
      Map.empty[String, String].boolean(showSplitCompilationTime, false) shouldBe true
    } finally {
      System.clearProperty(showSplitCompilationTime)
    }
  }

  // runs 12gb 1m15s. 0.12 ms / row, grouping takes 3s
  def doGrouped129ViaTopLevelBooleanGrouping(s: SparkSession): Unit = {
    import s.implicits._
    val res = doRuleTest(s, rules(s, genRules1to1(s, outputDir).as[(String, String, Int)]),
      "1:1 loaded but should group via TopLevelBooleanGrouper",
      extraConfig = Map(
        groupProcessorKey -> topLevelBooleanGrouper,
        showSplitCompilationTime -> "true",
        showGroupingTime -> "true",
        "statsEvery" -> "1000",
        groupProcessorPercentFilter -> "0.0012"
      ))

    val play = res.persist(StorageLevel.OFF_HEAP)

    play.filter("(k_out is null) or (k != k_out) or (l != l_out) or (l_out is null)").
      count() shouldBe 0
  }

  def doGrouped129ViaTopLevelBooleanGroupingEmpty(s: SparkSession): Unit = {
    import s.implicits._
    val res = doRuleTest(s, rules(s, genRules1to1(s, outputDir).as[(String, String, Int)]),
      "1:1 loaded but should group via TopLevelBooleanGrouper",
      extraConfig = Map(
        groupProcessorKey -> topLevelBooleanGrouper,
        showSplitCompilationTime -> "true",
        showGroupingTime -> "true",
        "statsEvery" -> "1000",
        useEmptyRuleSetResults -> "true"//,
        //groupProcessorPercentFilter -> "0.012"
      ))

    val play = res.persist(StorageLevel.OFF_HEAP)

    play.filter("(k_out is null) or (k != k_out) or (l != l_out) or (l_out is null)").
      count() shouldBe 0
  }

  def do1to1RulesOnly(s: SparkSession): Unit = {  // requires a 12gb heap and patience, run takes 5m42s on 32g i9-9900 corsair with 12gb, 5.22 ms / row
    import s.implicits._
/*    val res = doRuleTest(s, rules(s, genRules1to1(s, outputDir).as[(String, String, Int)]),
      "1:1 loaded direct cost",
      extraConfig = Map(
        showSplitCompilationTime -> "true",
        "statsEvery" -> "1000"
      ))*/


    var start = System.nanoTime()
    val d = s.read.option("header",true).csv(testFile(s,outputDir))
    val r = d.select(expr("*"), ruleEngineRunner(rules(s, genRules1to1(s, outputDir).as[(String, String, Int)]),
      extraConfig = Map(
        showSplitCompilationTime -> "true",
        "statsEvery" -> "1000"
      )).
      as("runner")).select(expr("*"), expr("runner.result.*"))
    var end = System.nanoTime()
    val typ ="1:1"
    println(s"$typ - took ${Duration.fromNanos(end - start).toSeconds}s to do logical plan")
  /*  start = System.nanoTime()
    r.limit(1).write.format("noop").mode(SaveMode.Overwrite).save()
    end = System.nanoTime()
    val compilationEstimation = Duration.fromNanos(end - start)
    println(s"$typ - took ${compilationEstimation.toMinutes}m${compilationEstimation.toSeconds % 60}s to do a limit 1, closest to compile time")
*/
    start = System.nanoTime()
    r.write.format("noop").mode(SaveMode.Overwrite).save()
    end = System.nanoTime()
    val fullDump = Duration.fromNanos(end - start)
    val processOf20kx20k = fullDump // - compilationEstimation
    println(s"$typ - took ${fullDump.toMinutes}m${fullDump.toSeconds % 60}s to do full noop write, of which" +
      s" ${processOf20kx20k.toMinutes}m${processOf20kx20k.toSeconds % 60}s in processing 20kx20k")
    r
/*
    val play = res.persist(StorageLevel.OFF_HEAP)

    play.filter("(k_out is null) or (k != k_out) or (l != l_out) or (l_out is null)").
      count() shouldBe 0*/
  }

  // this is a beast do by hand or on 16gb
  def doDumpAuditShouldWork(s: SparkSession): Unit = {
    import s.implicits._
    doRuleTest(s, rules(s, genRules1to1(s, outputDir).as[(String, String, Int)]),
      "dump audit via TopLevelBooleanGrouper",
      extraConfig = Map(
        groupProcessorKey -> classOf[TopLevelBooleanGrouper].getName,
        groupProcessorDumpAuditKey -> "true",
        groupProcessorAuditLocation -> outputDir,
        groupProcessorAuditMinBucket -> "1400",
        groupProcessorAuditMaxBucket -> "1500"
      ))

    val group = RuleSuiteGroupIOUtils.fromFile(outputDir + "/RuleEngineRunner")
    group.ruleSuites.size should be > 50

    // the 16k population is a single group converted to switch
    group.ruleSuites.count(_._2.ruleSets.head.rules.size < 200) shouldBe (group.ruleSuites.size - 1)

    // verify some of it is correct
    group.ruleSuites(Id(0,0)).ruleSets.exists(p => p.rules.exists{ r =>
      val s = r.toString // the exact match isn't possible when running all the tests, so the parts are searched for which should work on all runtimes
      s.contains("j = 'a199998'") && s.contains("f = 'a183365'") && s.contains("g = 'a199998'") &&
        s.contains("a = 'a199997'") && s.contains("d = 'a199999'") && s.contains("i = 'a199999'")
    }) shouldBe true
    group.ruleSuites.exists(_._2.ruleSets.exists(p => p.rules.exists(_.toString.contains("(f = 'a192967')")))) shouldBe true
    group.ruleSuites.exists(_._2.ruleSets.exists(p => p.rules.exists(_.toString.contains("((abs(hash(a, b, f)) % 3) = 1)")))) shouldBe true
  }

}

/**
 * Force the optimiser
 */
class BigRules extends ClassicSharedTests with BigRulesBase {

  override val runWith: ConnectionType = com.sparkutils.testing.ClassicOnly

  val useOptimiser: Boolean = true

  test("Trigger getValue should work") {
    doTriggerGetValueShouldWork(sparkSession)
  }

  test("grouped 129 via top level boolean grouping") { not3_0_or_3_1 { // runs 2g 1.5m. 0.04 ms / row (i9-9900), grouping takes 3s
    doGrouped129ViaTopLevelBooleanGrouping(sparkSession)
  } }

  test("grouped 129 via top level boolean grouping with empty result") { not3_0_or_3_1 { // runs 2g 1.5m. 0.04 ms / row (i9-9900), grouping takes 3s
    doGrouped129ViaTopLevelBooleanGroupingEmpty(sparkSession)
  } }

  // pre 0.2.0 would require a 12gb heap and patience with > 5m runs, run on 0.2.0 takes sub 2m on 32g i9-9900 corsair with 12gb heap, sub 3 ms / row
  // running the same test on 0.1.3.1 is 2.5m minimum with > 6ms / row
  // not_Cluster as the serialisation of the plan to executors requires at least a 64gb node type.
  test("1:1 rules only") { not_Cluster {
    do1to1RulesOnly(sparkSession)
  } }

  test("dumpAudit should work") { not3_0_or_3_1 {
    doDumpAuditShouldWork(sparkSession)
  } }

}
