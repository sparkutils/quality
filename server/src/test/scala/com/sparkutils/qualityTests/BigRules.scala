package com.sparkutils.qualityTests

import com.sparkutils.quality._
import com.sparkutils.quality.impl.util.RuleSuiteGroupIOUtils
import com.sparkutils.quality.impl.{TopLevelBooleanGrouper, Triggers}
import com.sparkutils.qualityTests.RulesGen.{genRules1to1, testfile}
import com.sparkutils.qualityTests.util.SharedPureConnectTests
import com.sparkutils.testing.ConnectionType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.scalatest.Matchers

import scala.concurrent.duration.Duration
import scala.util.Try

object RulesGen {

  val testfile = getClass.getResource("/20k_rule_suite.csv").getPath

  // only available on 3.5
  val replace = org.apache.spark.sql.functions.udf((source: String, against: String, withWhat: String) =>
    source.replace(against, withWhat))

  val f_bucket_size = 40
  val fModExpr = s"if(f = '*', 0, hash(f) % $f_bucket_size)"

  def genRules1to1(s: SparkSession, withF: Boolean = false) = {

    val d = s.read.option("header",true).csv(testfile)
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

class BigRules extends SharedPureConnectTests with Matchers {

  override val runWith: ConnectionType = com.sparkutils.testing.ClassicOnly

  def rules(dataSet: Dataset[(String, String, Int)]) = {
    val s = sparkSession

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

  def doRuleTest(ruleSuite: RuleSuite, typ: String, resultDataType: Option[DataType] = Some(
    StructType(Seq(
      StructField("k_out", StringType),
      StructField("l_out", StringType)
    )
  )), topLevelRunner: (RuleSuite, Option[DataType], Map[String, String]) => Column =
      (rs, dt, op) => ruleEngineRunner(rs, dt, extraConfig = op), processor: DataFrame => DataFrame =
        _.select(expr("*"), expr("runner.result.*")), extraConfig: Map[String, String] = Map.empty): DataFrame = {
    var start = System.nanoTime()
    val s = sparkSession
    val d = s.read.option("header",true).csv(testfile)
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

  test("Trigger getValue should work") {
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

  test("grouped 129 via top level boolean grouping") { // runs in 4gb, at 12gb 2m44. 0.464 ms / row, grouping takes 3s
    val s = sparkSession

    import s.implicits._
    val res = doRuleTest(rules(genRules1to1(sparkSession).as[(String, String, Int)]),
      "1:1 loaded but should group via TopLevelBooleanGrouper",
      extraConfig = Map(
        groupProcessorKey -> classOf[TopLevelBooleanGrouper].getName,
        showSplitCompilationTime -> "true",
        showGroupingTime -> "true",
        "statsEvery" -> "1000"
      ))

    val play = res.cache

    play.filter("(k_out is null) or (k != k_out) or (l != l_out) or (l_out is null)").
      count() shouldBe 0
  }

  ignore("1:1 rules only") {  // requires a 12gb heap and patience, run takes 5m42s on 32g i9-9900 corsair with 12gb, 5.22 ms / row
    val s = sparkSession

    import s.implicits._
    val res = doRuleTest(rules(genRules1to1(sparkSession).as[(String, String, Int)]),
      "1:1 loaded direct cost",
      extraConfig = Map(
        showSplitCompilationTime -> "true",
        "statsEvery" -> "1000"
      ))

    val play = res.cache

    play.filter("(k_out is null) or (k != k_out) or (l != l_out) or (l_out is null)").
      count() shouldBe 0
  }

  test("dumpAudit should work") {
    val s = sparkSession

    import s.implicits._
    doRuleTest(rules(genRules1to1(sparkSession).as[(String, String, Int)]),
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