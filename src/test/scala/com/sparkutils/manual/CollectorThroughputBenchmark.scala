package com.sparkutils.manual

import com.sparkutils.quality
import com.sparkutils.quality.{ExpressionRule, Id, OutputExpression, Rule, RuleSet, RuleSuite, RunOnPassProcessor}
import com.sparkutils.qualityTests.RowTools
import org.apache.spark.sql.functions.{explode, expr}
import org.apache.spark.sql.types.{ArrayType, LongType}
import org.apache.spark.sql.{Column, Row, SaveMode}
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
  def main(args: Array[String]) = {

    sparkSession.range(1000000).repartition(4).write.mode(SaveMode.Overwrite).parquet(outputDir + "/collectTestData")

  }
}

// make sure to run TestSetup first
// Quality comes ahead after 1m rows at 30 rules, for 10m rows it's always faster
// (collectRunner is 30% faster than folder and 50% more than pure spark)
// compilation is impossible with 64k issue on more than 50 rules for Spark, no issue for Quality
object CollectorThroughputBenchmark extends Bench.OfflineReport with RowTools {

  def evaluate[T](colF: Int => Column, result: => Column)(param: Int) = {
    val col = colF(param)
    val df = sparkSession.read.parquet(outputDir + "/collectTestData")
    df.select(col).select(explode(result).as("result")).count() // enough to force
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

  // not possible to run on pure spark with 50, 200, 50 even on 4 it hits the 64kb problem
  val rules = Gen.range("ruleCount")(10, 50, 10)

  performance of "Processing Array Collection Transformations" config (
    exec.minWarmupRuns -> 2,
    exec.maxWarmupRuns -> 4,
    exec.benchRuns -> 4,
    exec.jvmcmd -> (System.getProperty("java.home")+"/bin/java"),
    exec.jvmflags -> Args.args
    //  verbose -> true
  ) in {
    sparkSession.conf
    quality.registerQualityFunctions()

    // actually faster than no filter, as it does not need to create as many arrays due to the filter,
    // so array creation is more impactful than the lambda cost
    measure method "pure spark passes only" in {
      val s = sparkSession

      val ruleCol = (rules: Int) => expr(s"flatten(filter(${pureSparkArray(rules)}, x -> x IS NOT NULL))").as("result")
      using(rules) in evaluate( ruleCol, expr("result") )
    }

    measure method "collect flatten remove nulls" in {
      val s = sparkSession

      val ruleCol = (numRules: Int) =>
        quality.collectRunner(genRules(numRules),
          Some(ArrayType(LongType, true)), flatten = true, includeNulls = false).as("result")

      using(rules) in evaluate( ruleCol, expr("result.result") )
    }

/*
    measure method "pure spark no null filter" in {
      val s = sparkSession

      val ruleCol = (rules: Int) => expr(s"flatten(${pureSparkArray(rules)})").as("result")
      using(rules) in evaluate( ruleCol, expr("result") )
    }
*/
    // overhead of 2 arrays and array copy rule per row with additional result array gen.  Also impact of not having sub
    // expression elimination as lambdas are not optimised.  Extra overhead of quality also included
    measure method "folder" in {
      val s = sparkSession

      val ruleCol = (numRules: Int) =>
        quality.ruleFolderRunner(genRules(numRules, "set( value = concat(currentResult.value, ", "))"),
          expr("named_struct('value', array(id))")).as("result")

      using(rules) in evaluate( ruleCol, expr("result.result.value") )
    }

  }


}
