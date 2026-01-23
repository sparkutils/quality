package com.sparkutils.manual

import com.sparkutils.quality.{ExpressionRule, Id, OutputExpression, Rule, RuleSet, RuleSuite, RunOnPassProcessor}
import com.sparkutils.qualityTests.RowTools
import org.apache.spark.sql.Row
import org.scalameter.api.{Bench, _}

object CollectorThroughputBenchmark extends Bench.OfflineReport with RowTools {

  def evaluate[T](processor: Int => Unit)(params: (Int, Int)) = {
    val inst = processor(params._1, params._2)
    val row = Row((0L to params._2.toLong).toIndexedSeq : _*)
    for{i <- 0 until writeRows} {
      inst(row)
    }
  }

  def genRules(rules: Int) =
    RuleSuite(Id(1, 1), Seq(RuleSet(Id(50, 1),
      (
        for{
          r <- 2 to rules
        } yield
          Rule(Id(50+r, 1),
            ExpressionRule(s"(id % 2) = 0 and (id % $r) = 0"),
            RunOnPassProcessor(r, Id(500+r, 1), OutputExpression(s"set( value = array_append(current.value, array(id, id + 1, id + 2, id + $r))"))
          )
      ) ++ Seq( // always run
        Rule(Id(50+rules + 1, 1),
          ExpressionRule(s"true"),
          RunOnPassProcessor(500+rules, Id(500+rules, 1), OutputExpression("set( value = array_append(current.value, array(id, id + 7, id + 4, id + 9))"))
        )
    )))
    )

  performance of "processingTransformation" config (
    exec.minWarmupRuns -> 2,
    exec.maxWarmupRuns -> 4,
    exec.benchRuns -> 4,
    exec.jvmcmd -> (System.getProperty("java.home")+"/bin/java"),
    exec.jvmflags -> List("-Xmx24g","-Xms24g")
    //  verbose -> true
  ) in {

    // overhead of 2 arrays and array copy rule per row with additional result array gen.  Also impact of not having sub
    // expression elimination as lambdas are not optimised
    measure method "folder" in {
      val s = sparkSession

      val
      using(generator) in evaluate( processor )
    }

  }


}
