package com.sparkutils.manual

import com.sparkutils.quality.sparkless.{ProcessFunctions, Processor}
import com.sparkutils.qualityTests.ResultHelper.longSchema
import com.sparkutils.qualityTests.RowTools
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.apache.spark.sql.{Row, ShimUtils}
import org.scalameter.api.Bench
import org.scalameter.api._

object ProcessorThroughputBenchmark extends Bench.OfflineReport with RowTools {

  def evaluate[T](processor: (Int, Int) => Processor[Row, T])(params: (Int, Int)) = {
    val inst = processor(params._1, params._2)
    val row = Row((0L to params._2.toLong).toIndexedSeq : _*)
    for{i <- 0 until writeRows} {
      inst(row)
    }
  }

  def startup[T](processor: (Int, Int) => Processor[Row, T])(params: (Int, Int)) = {
    processor(params._1, params._2)
  }

  performance of "processingDQ" config (
    exec.minWarmupRuns -> 2,
    exec.maxWarmupRuns -> 4,
    exec.benchRuns -> 4,
    exec.jvmcmd -> (System.getProperty("java.home")+"/bin/java"),
    exec.jvmflags -> List("-Xmx24g","-Xms24g")
    //  verbose -> true
  ) in {

    measure method "VarCompilation" in {
      val s = sparkSession

      val processor = (rules: Int, cols: Int) => {
        implicit val renc = ShimUtils.rowEncoder(longSchema(cols, LongType))
        ProcessFunctions.dqFactory[Row](genRules(rules, cols)).instance
      }
      using(generator) in evaluate( processor )
    }

    measure method "VarCompilationStartup" in {
      val s = sparkSession

      val processor = (rules: Int, cols: Int) => {
        implicit val renc = ShimUtils.rowEncoder(longSchema(cols, LongType))
        ProcessFunctions.dqFactory[Row](genRules(rules, cols)).instance
      }
      using(generator) in startup( processor )
    }

    measure method "CompiledProjections" in {
      val s = sparkSession

      val processor = (rules: Int, cols: Int) => {
        implicit val renc = ShimUtils.rowEncoder(longSchema(cols, LongType))
        ProcessFunctions.dqFactory[Row](genRules(rules, cols),
          forceVarCompilation = false).instance
      }
      using(generator) in evaluate( processor )
    }

    measure method "CompiledProjectionsStartup" in {
      val s = sparkSession

      val processor = (rules: Int, cols: Int) => {
        implicit val renc = ShimUtils.rowEncoder(longSchema(cols, LongType))
        ProcessFunctions.dqFactory[Row](genRules(rules, cols),
          forceVarCompilation = false).instance
      }
      using(generator) in startup( processor )
    }

    measure method "MutableProjectionsCompiled" in {
      val s = sparkSession

      val processor = (rules: Int, cols: Int) => {
        implicit val renc = ShimUtils.rowEncoder(longSchema(cols, LongType))
        ProcessFunctions.dqFactory[Row](genRules(rules, cols),
          forceMutable = true).instance
      }
      using(generator) in evaluate( processor )
    }

    measure method "MutableProjectionsCompiledStartup" in {
      val s = sparkSession

      val processor = (rules: Int, cols: Int) => {
        implicit val renc = ShimUtils.rowEncoder(longSchema(cols, LongType))
        ProcessFunctions.dqFactory[Row](genRules(rules, cols),
          forceMutable = true).instance
      }
      using(generator) in startup( processor )
    }

    measure method "interpreted" in {
      val s = sparkSession

      val processor = (rules: Int, cols: Int) => {
        implicit val renc = ShimUtils.rowEncoder(longSchema(cols, LongType))
        ProcessFunctions.dqFactory[Row](genRules(rules, cols),
          compile = false).instance
      }
      using(generator) in evaluate( processor )
    }
    measure method "interpretedStartup" in {
      val s = sparkSession

      val processor = (rules: Int, cols: Int) => {
        implicit val renc = ShimUtils.rowEncoder(longSchema(cols, LongType))
        ProcessFunctions.dqFactory[Row](genRules(rules, cols),
          compile = false).instance
      }
      using(generator) in startup( processor )
    }

  }
}
