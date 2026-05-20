package com.sparkutils.qualityTests.classicOnly

import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem
import com.sparkutils.quality._
import com.sparkutils.quality.classicFunctions.enableOptimizations
import com.sparkutils.quality.impl.extension.{QualitySparkExtension, ZeroCodeGenRule}
import com.sparkutils.quality.impl.util.RuleSuiteGroupIOUtils
import com.sparkutils.quality.impl.{TopLevelBooleanGrouper, Triggers}
import com.sparkutils.qualityTests.classicOnly.RulesGen.{genRules1to1, testFile}
import com.sparkutils.qualityTests.util.SharedPureConnectTests
import com.sparkutils.testing.{ConnectionType, TestUtilsEnvironment}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.local.BareStreamingLocalFileSystem
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.scalatest.{Matchers, TestSuite, fixture}

import scala.concurrent.duration.Duration
import scala.util.Try

object RulesGen {

  def testfileResource = getClass.getResourceAsStream("/20k_rule_suite.csv")

  // only available on 3.5
  val replace = org.apache.spark.sql.functions.udf((source: String, against: String, withWhat: String) =>
    source.replace(against, withWhat))

  val f_bucket_size = 40
  val fModExpr = s"if(f = '*', 0, hash(f) % $f_bucket_size)"

  def testFile(s: SparkSession, outputDir: String): String = {
    val tmp = outputDir + "/20k_rule_suite.csv"
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

}

trait BigRulesBase extends Matchers {

  def outputDir: String

  val useOptimiser: Boolean

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

    if (useOptimiser) {
      enableOptimizations(Seq(ZeroCodeGenRule))
    }

    var start = System.nanoTime()
    val d = s.read.option("header",true).csv(testFile(s,outputDir))
    val r = processor(d.select(expr("*"), topLevelRunner(ruleSuite, resultDataType, extraConfig).
      as("runner")))
    var end = System.nanoTime()

    println(s"$typ - took ${Duration.fromNanos(end - start).toSeconds}s to do logical plan")
    start = System.nanoTime()
    r.write.format("noop").mode(SaveMode.Overwrite).save()
    end = System.nanoTime()
    val fullDump = Duration.fromNanos(end - start)
    println(s"$typ - took ${fullDump.toMinutes}m${fullDump.toSeconds % 60}s to do full noop write")

    r
  }

  def doTriggerGetValueShouldWork(s: SparkSession): Unit = {
    Try(Triggers.getValue(showSplitCompilationTime, Map.empty, "false").toBoolean).getOrElse(false) shouldBe false

    Try(Triggers.getValue(showSplitCompilationTime, Map(
      showSplitCompilationTime -> "true"
    ), "false").toBoolean).getOrElse(false) shouldBe true

    try {
      System.setProperty(showSplitCompilationTime, "true")
      Try(Triggers.getValue(showSplitCompilationTime, Map.empty, "false").toBoolean).getOrElse(false) shouldBe true
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
        groupProcessorKey -> classOf[TopLevelBooleanGrouper].getName,
        showSplitCompilationTime -> "true",
        showGroupingTime -> "true",
        "statsEvery" -> "1000"
      ))

    val play = res.persist(StorageLevel.OFF_HEAP)

    play.filter("(k_out is null) or (k != k_out) or (l != l_out) or (l_out is null)").
      count() shouldBe 0
  }

  def do1to1RulesOnly(s: SparkSession): Unit = {  // requires a 12gb heap and patience, run takes 5m42s on 32g i9-9900 corsair with 12gb, 5.22 ms / row
    import s.implicits._
    val res = doRuleTest(s, rules(s, genRules1to1(s, outputDir).as[(String, String, Int)]),
      "1:1 loaded direct cost",
      extraConfig = Map(
        showSplitCompilationTime -> "true",
        "statsEvery" -> "1000"
      ))

    val play = res.persist(StorageLevel.OFF_HEAP)

    play.filter("(k_out is null) or (k != k_out) or (l != l_out) or (l_out is null)").
      count() shouldBe 0
  }

  // this is a beast do by hand or on 16gb
  def doDumpAuditShouldWork(s: SparkSession): Unit = {
    import s.implicits._
    doRuleTest(s, rules(s, genRules1to1(s, outputDir).as[(String, String, Int)]),
      "dump audit via TopLevelBooleanGrouper",
      extraConfig = Map(
        groupProcessorKey -> classOf[TopLevelBooleanGrouper].getName,
        groupProcessorAuditKey -> "true",
        groupProcessorAuditLocation -> outputDir
      ))

    val group = RuleSuiteGroupIOUtils.fromFile(outputDir + "/RuleEngineRunner")
    group.ruleSuites.size shouldBe 181

    // verify some of it is correct
    group.ruleSuites(Id(0,0)).ruleSets.exists(p => p.rules.exists(_.toString.contains("hash(a, b, c, f)"))) shouldBe true
  }

}

/**
 * Force the optimiser
 */
class BigRules extends SharedPureConnectTests with BigRulesBase {

  override val runWith: ConnectionType = com.sparkutils.testing.ClassicOnly

  val useOptimiser: Boolean = true

  test("Trigger getValue should work") {
    doTriggerGetValueShouldWork(sparkSession)
  }

  test("grouped 129 via top level boolean grouping") { not3_0_or_3_1 { // runs 12gb 1m15s. 0.12 ms / row, grouping takes 3s
    doGrouped129ViaTopLevelBooleanGrouping(sparkSession)
  } }

  ignore("1:1 rules only") { // requires a 12gb heap and patience, run takes 5m42s on 32g i9-9900 corsair with 12gb, 5.22 ms / row
    do1to1RulesOnly(sparkSession)
  }

  ignore("dumpAudit should work") { not3_0_or_3_1 { // this is a beast do by hand or on 16gb
    doDumpAuditShouldWork(sparkSession)
  } }

}

/*
/**
 * Only runs on OSS, Databricks cannot manage this
 */
class BigRulesOSS extends SharedPureConnectTests with BigRulesBase {

  val useOptimiser: Boolean = false

  override val runWith: ConnectionType = com.sparkutils.testing.ClassicOnly

  test("Trigger getValue should work") { not_Cluster {
    doTriggerGetValueShouldWork(sparkSession)
  } }

  test("grouped 129 via top level boolean grouping") { not_Cluster { not3_0_or_3_1 { // runs 12gb 1m15s. 0.12 ms / row, grouping takes 3s
    doGrouped129ViaTopLevelBooleanGrouping(sparkSession)
  } } }

  ignore("1:1 rules only") { not_Cluster{ // requires a 12gb heap and patience, run takes 5m42s on 32g i9-9900 corsair with 12gb, 5.22 ms / row
    do1to1RulesOnly(sparkSession)
  } }

  ignore("dumpAudit should work") { not_Cluster{ not3_0_or_3_1 { // this is a beast do by hand or on 16gb
    doDumpAuditShouldWork(sparkSession)
  } } }

}

/**
 * Only runs on OSS, probably can't run on CI.  No easy way to see it's been run through
 * [[com.sparkutils.quality.impl.extension.ZeroCodeGen]] other than to actually debug
 */
class BigRulesOSSNewSessionWithExtension extends SharedPureConnectTests with BigRulesBase {

  override val runWith: ConnectionType = com.sparkutils.testing.ClassicOnly

  val useOptimiser: Boolean = false

  override def sparkSession: SparkSession = {

    {
      val builder = SparkSession.builder()
      if (System.getProperty("os.name").startsWith("Windows"))
        builder.config("spark.hadoop.fs.file.impl", classOf[BareLocalFileSystem].getName).
          config("spark.hadoop.fs.AbstractFileSystem.file.impl", classOf[BareStreamingLocalFileSystem].getName)
      else
        builder
    }.withExtensions(new QualitySparkExtension).create()

  }

  def newExtensionSession(thunk: SparkSession => Unit): Unit = not_Cluster {
    val s = sparkSession
    try {
      thunk(s)
    } finally {
      s.close()
    }
  }

  test("Trigger getValue should work") { newExtensionSession { sparkSession =>
    doTriggerGetValueShouldWork(sparkSession)
  } }

  // runs 12gb 1m15s. 0.12 ms / row, grouping takes 3s
  test("grouped 129 via top level boolean grouping") { not3_0_or_3_1 { newExtensionSession { sparkSession =>
    doGrouped129ViaTopLevelBooleanGrouping(sparkSession)
  } } }

  // requires a 12gb heap and patience, run takes 5m42s on 32g i9-9900 corsair with 12gb, 5.22 ms / row
  ignore("1:1 rules only") { newExtensionSession { sparkSession =>
    do1to1RulesOnly(sparkSession)
  } }

  // this is a beast do by hand or on 16gb
  ignore("dumpAudit should work") { not3_0_or_3_1 { newExtensionSession { sparkSession =>
    doDumpAuditShouldWork(sparkSession)
  } } }

}

/**
 * For running when the extensions are enabled.  The connect test cannot work as it takes too much memory so BigRuleOSS
 * will be used on CI, this for shade only
 */
class BigRulesOnClusterExtensions extends SharedPureConnectTests with BigRulesBase {

  override val runWith: ConnectionType = com.sparkutils.testing.ClassicOnly

  val useOptimiser: Boolean = false

  def onlyOnClusterExtension(thunk: => Unit): Unit = onlyWithExtension {
    if (TestUtilsEnvironment.onDatabricksFS || TestUtilsEnvironment.onFabricOrSynapse(sparkSession)) {
      thunk
    }
  }

  test("Trigger getValue should work") { onlyOnClusterExtension {
    doTriggerGetValueShouldWork(sparkSession)
  } }

  test("grouped 129 via top level boolean grouping") { onlyOnClusterExtension { not3_0_or_3_1 { // runs 12gb 1m15s. 0.12 ms / row, grouping takes 3s
    doGrouped129ViaTopLevelBooleanGrouping(sparkSession)
  } } }

  ignore("1:1 rules only") { not_Cluster{ // requires a 12gb heap and patience, run takes 5m42s on 32g i9-9900 corsair with 12gb, 5.22 ms / row
    do1to1RulesOnly(sparkSession)
  } }

  ignore("dumpAudit should work") { onlyOnClusterExtension { not3_0_or_3_1 { // this is a beast do by hand or on 16gb
    doDumpAuditShouldWork(sparkSession)
  } } }

}

*/