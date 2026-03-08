package com.sparkutils.manual

import com.sparkutils.manual.StatisticsThroughputBenchmarkTestSetup.generator
import com.sparkutils.quality
import com.sparkutils.quality.functions.{rule_suite_statistics, rule_suite_statistics_aggregator}
import com.sparkutils.quality.{DefaultRule, DisabledRule, Failed, Id, IgnoredRule, Passed, Probability, RuleSetResult, RuleSuiteResult, SoftFailed, registerQualityFunctions}
import com.sparkutils.testing.{ClassicOnly, Sessions, TestUtils}
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{Column, Dataset, Row, SaveMode}
import org.apache.spark.storage.StorageLevel
import org.scalameter.Gen.crossProduct
import org.scalameter.api.{Bench, _}

object StatisticsThroughputBenchmarkTestSetup extends TestUtils {

  val r =
    RuleSuiteResult(
      Id(100,0), Passed, Map(
        Id(1,0) ->
          RuleSetResult(DisabledRule, Map(
            Id(1,0) -> Failed,
            Id(2,0) -> Passed,
            Id(3,0) -> IgnoredRule,
            Id(4,0) -> DefaultRule,
            Id(5,0) -> SoftFailed,
            Id(6,0) -> DisabledRule,
            Id(7,0) -> Probability(0.4),
            Id(8,0) -> Probability(0.9)
          ))
      ))

  val data = Seq(("a", r))

  val partitions = Gen.range("partitions")(10, 50, 10)
  val rows = Gen.range("rulesetCount")(100000, 1000000, 100000)
  // for memory usage, hardly any difference, most time likely in shuffling
  // val rows = Gen.range("rulesetCount")(100000, 1000000, 100000)

  val generator = crossProduct(rows, partitions).map(p =>
    readdf(p._1, p._2)
  ).cached

  def readdf(rows: Int, partitions: Int) = {
    quality.registerQualityFunctions()

    sparkSession.read.parquet(outputDir + s"/statsthroughput_${rows}_$partitions")
  }

  // below is enough on Spark 3.5 to force compilation
  def memdf(rows: Long, partitions: Int) = {
    val s = sparkSession
    quality.registerQualityFunctions()
    import s.implicits.localSeqToDatasetHolder
    import com.sparkutils.quality.implicits._
    import frameless._

    implicit val enc = TypedExpressionEncoder[(String, RuleSuiteResult)]

    val df = localSeqToDatasetHolder[(String, RuleSuiteResult)](data).toDS()

    s.range(rows).join(df).repartition(partitions).persist(StorageLevel.MEMORY_ONLY)//write.mode(SaveMode.Overwrite).parquet(outputDir + s"/statsthroughput_${rows}_$partitions")
  }

  // below is enough on Spark 3.5 to force compilation
  def writedf(rows: Long, partitions: Int) = {
    val s = sparkSession
    quality.registerQualityFunctions()
    import s.implicits.localSeqToDatasetHolder
    import com.sparkutils.quality.implicits._
    import frameless._

    registerQualityFunctions()

    implicit val enc = TypedExpressionEncoder[(String, RuleSuiteResult)]

    val df = localSeqToDatasetHolder[(String, RuleSuiteResult)](data).toDS()

    s.range(rows).join(df).repartition(partitions).write.mode(SaveMode.Overwrite).parquet(outputDir + s"/statsthroughput_${rows}_$partitions")
    ()
  }

  override def sessions: Sessions =  createSparkSessions(ClassicOnly)

  def main(args: Array[String]): Unit = {
    val g = crossProduct(rows, partitions).map( p => {
      writedf(p._1, p._2)
    })
    g.dataset.foreach{
      p =>
        g.generate(p)
    }
  }
}

/**
 * Make sure to run StatisticsThroughputBenchmarkTestSetup first to create the files
 */
object StatisticsThroughputBenchmark extends Bench.OfflineReport with TestUtils {

  def evaluate(colF: Column => Column)(params: Dataset[Row]) = {
    val col = colF(expr("_2"))
    //val t = sparkSession.read.parquet(outputDir + s"/statsthroughput_${params._1}_${params._2}")
    //t.printSchema()
    params.select(col).write.format("noop").mode(Overwrite).save()
  }

  performance of "Processing statistics" config (
    exec.minWarmupRuns -> 2,
    exec.maxWarmupRuns -> 4,
    exec.benchRuns -> 4,
    exec.jvmcmd -> (System.getProperty("java.home")+"/bin/java"),
    exec.jvmflags -> Args.args
    //  verbose -> true
  ) in {

    measure method "default declarative" in {
      val s = sparkSession

      using(generator) in evaluate( rule_suite_statistics )
    }

    measure method "aggregator" in {
      val s = sparkSession

      using(generator) in evaluate( rule_suite_statistics_aggregator )
    }
  }

  override def sessions: Sessions =  createSparkSessions(ClassicOnly)
}
