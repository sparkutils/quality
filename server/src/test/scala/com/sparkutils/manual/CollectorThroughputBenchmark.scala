package com.sparkutils.manual

import com.sparkutils.manual.TestSetup.sparkSession
import com.sparkutils.quality
import com.sparkutils.quality.impl.CollectRunner
import com.sparkutils.quality.{ExpressionRule, Id, OutputExpression, Rule, RuleSet, RuleSuite, RunOnPassProcessor}
import com.sparkutils.qualityTests.util.{ClassicSharedTests, RowTools, TestUtilsBase}
import com.sparkutils.testing.markers.DontRunOnPureConnect
import com.sparkutils.testing.sessionStrategies.{GlobalSession, SharedSessions}
import com.sparkutils.testing.{ClassicOnly, ClassicUtils, ConnectionType, Sessions, SessionsStateHolder, SparkTestSuite, TestUtils, TestUtilsEnvironment}
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.functions.{explode, expr}
import org.apache.spark.sql.types.{ArrayType, LongType}
import org.apache.spark.sql.{Column, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.scalameter.api.{Bench, _}

object Args {
  val args = List(
    "-Xmx10g","-Xms10g",// 16GB on github runners, 10gb ok on 21 (12 blows), 12gb fine on jdk 8.
    "-ea",
    "-XX:+IgnoreUnrecognizedVMOptions",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
  )
}

object TestSetup extends RowTools {
  //val ROWS = 10000000
  val ROWS = 1000000
  //val ROWS = 100000
  //val ROWS = 10000

  def main(args: Array[String]) = {

    sparkSession.range(ROWS).repartition(4).write.mode(SaveMode.Overwrite).parquet(outputDir + "/collectTestData")

  }

  override val runWith: ConnectionType = ClassicOnly

  override def sessions: Sessions = createSparkSessions(connectionType)
}

// make sure to run TestSetup first
// Quality comes ahead after 1m rows at 30 rules, for 10m rows it's always faster
// (collectRunner is 30% faster than folder and 50% more than pure spark)
// compilation is impossible with 64k issue on more than 50 rules for Spark, no issue for Quality
object CollectorThroughputBenchmark extends Bench.OfflineReport with TestUtils {
/*
  override val currentSessionsHolder: SessionsStateHolder = GlobalSession

  override val runWith: ConnectionType = ClassicOnly
*/
  // below is enough on Spark 3.5 to force compilation
  def df = {
    val s = sparkSession
    quality.registerQualityFunctions()

    s.range(TestSetup.ROWS).repartition(4).persist(StorageLevel.MEMORY_ONLY)
  } //sparkSession.read.parquet(outputDir + "/collectTestData")

  def evaluate[T](colF: Int => Column, result: => Column)(param: Int) = {
    val col = colF(param)
    df.select(col).write.format("noop").mode(Overwrite).save()
  }

  def genRules(rules: Int, prefix: String = "", postfix: String = "") =
    RuleSuite(Id(1, 1), Seq(RuleSet(Id(50, 1),
      (
        for{
          r <- 2 to rules
        } yield
          Rule(Id(50+r, 1),
            ExpressionRule(s"(id % 2) = 0 and (id % $r) = 0"),
            RunOnPassProcessor(r, Id(500+r, 1), OutputExpression(s"$prefix array(${
              if (r % 5 == 0) "null" else "id"
            }, id + 1, id + 2, id + $r)$postfix"))
          )
      ) ++ Seq( // always run
        Rule(Id(50+rules + 1, 1),
          ExpressionRule(s"true"),
          RunOnPassProcessor(500 + rules + 1, Id(500 + rules + 1, 1),
            OutputExpression(s"$prefix array(id, id + 7, id + 4, id + 9)$postfix"))
        )
    )))
    )

  def pureSparkArray(rules: Int) =
    s"""
     array(
     ${(
      for{
        r <- 2 to rules
      } yield
        s"if((id % 2) = 0 and (id % $r) = 0, array(${
          if (r % 5 == 0) "null" else "id"
          }, id + 1, id + 2, id + $r), null)"

    ) ++ Seq( // always run
      s"array(id, id + 7, id + 4, id + 9)"
    ) mkString(",")
})"""

  // not possible to run on pure spark with 50, 200, 50 even on 4 it hits the 64kb problem // memory wise 10, 50, 10 only starts to stress things at 50 on 100k rows
  val rules = Gen.range("ruleCount")(100, 100, 1)

  performance of "Processing Array Collection Transformations" config (
    exec.minWarmupRuns -> 2,
    exec.maxWarmupRuns -> 4,
    exec.benchRuns -> 4,
    exec.jvmcmd -> (System.getProperty("java.home")+"/bin/java"),
    exec.jvmflags -> Args.args
    //  verbose -> true
  ) in {

    // actually faster than no filter, as it does not need to create as many arrays due to the filter,
    // so array creation is more impactful than the lambda cost
  /* */
/*
    measure method "pure spark passes only" in {
      val s = sparkSession

      val ruleCol = (rules: Int) => expr(s"flatten(filter(${pureSparkArray(rules)}, x -> x IS NOT NULL))").as("result")
      using(rules) in evaluate( ruleCol, expr("result") )
    }
*/
    measure method "collect flatten remove nulls" in {
      val s = sparkSession

      val ruleCol = (numRules: Int) =>
        CollectRunner.collectRunnerClassic(genRules(numRules),
          Some(ArrayType(LongType, true)), flatten = true, includeNulls = false).as("result")

      using(rules) in evaluate( ruleCol, expr("result.result") )
    }

    measure method "collect flatten remove nulls - no Inplace" in {
      val s = sparkSession

      val ruleCol = (numRules: Int) =>
        CollectRunner.collectRunnerClassic(genRules(numRules),
          Some(ArrayType(LongType, true)), flatten = true, includeNulls = false).as("result")

      val f = (p: Int) =>
        try {
          System.setProperty(CollectRunner.UseInPlaceArray, "false")
          evaluate( ruleCol, expr("result.result") )(p)
        } finally {
          System.clearProperty(CollectRunner.UseInPlaceArray)
        }

      using(rules) in f
    }

    measure method "collect flatten remove nulls with unroll at 5" in {
      val s = sparkSession

      val ruleCol = (numRules: Int) =>
        CollectRunner.collectRunnerClassic(genRules(numRules),
          Some(ArrayType(LongType, true)), flatten = true, includeNulls = false).as("result")

      val f = (p: Int) =>
        try {
          System.setProperty(CollectRunner.UnrollOutputArray, "true")
          System.setProperty(CollectRunner.UnrollOutputArraySize, "5")
          evaluate( ruleCol, expr("result.result") )(p)
        } finally {
          System.clearProperty(CollectRunner.UnrollOutputArray)
        }

      using(rules) in f
    }

    measure method "collect flatten remove nulls with unroll at 2" in {
      val s = sparkSession

      val ruleCol = (numRules: Int) =>
        CollectRunner.collectRunnerClassic(genRules(numRules),
          Some(ArrayType(LongType, true)), flatten = true, includeNulls = false).as("result")

      val f = (p: Int) =>
        try {
          System.setProperty(CollectRunner.UnrollOutputArray, "true")
          System.setProperty(CollectRunner.UnrollOutputArraySize, "5")
          evaluate( ruleCol, expr("result.result") )(p)
        } finally {
          System.clearProperty(CollectRunner.UnrollOutputArray)
        }

      using(rules) in f
    }
/*
    measure method "collect flatten remove nulls - no Inplace - no unroll" in {
      val s = sparkSession

      val ruleCol = (numRules: Int) =>
        CollectRunner.collectRunnerClassic(genRules(numRules),
          Some(ArrayType(LongType, true)), flatten = true, includeNulls = false).as("result")

      val f = (p: Int) =>
        try {
          System.setProperty(CollectRunner.UseInPlaceArray, "false")
          System.setProperty(CollectRunner.UnrollOutputArray, "false")
          evaluate( ruleCol, expr("result.result") )(p)
        } finally {
          System.clearProperty(CollectRunner.UseInPlaceArray)
          System.clearProperty(CollectRunner.UnrollOutputArray)
        }

      using(rules) in f
    }
*/
    /*
        measure method "pure spark no null filter" in {
          val s = sparkSession

          val ruleCol = (rules: Int) => expr(s"flatten(${pureSparkArray(rules)})").as("result")
          using(rules) in evaluate( ruleCol, expr("result") )
        }
    */
    // overhead of 2 arrays and array copy rule per row with additional result array gen.  Also impact of not having sub
    // expression elimination as lambdas are not optimised.  Extra overhead of quality also included
/* */ /*
    measure method "folder" in {
      val s = sparkSession

      val ruleCol = (numRules: Int) =>
        quality.ruleFolderRunner(genRules(numRules, "set( value = concat(currentResult.value, ", "))"),
          expr("named_struct('value', array(id))")).as("result")

      using(rules) in evaluate( ruleCol, expr("result.result.value") )
    }
*/
  }

//  override def sparkSession: SparkSession = classicSparkSession.get

  override def sessions: Sessions =  createSparkSessions(ClassicOnly)
}
