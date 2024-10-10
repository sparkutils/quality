package com.sparkutils.qualityTests

import com.sparkutils.quality.impl.RuleLogicUtils.mapRules
import com.sparkutils.quality._
import types._
import com.sparkutils.quality.functions.rng_bytes
import com.sparkutils.quality.impl.rng.RandomBytes
import com.sparkutils.qualityTests.ResultHelper.longSchema
import org.apache.commons.rng.simple.RandomSource
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, QualitySparkUtils, Row, ShimUtils, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.scalameter.api.Gen.crossProduct
import org.scalameter.api._

import scala.collection.JavaConverters._

trait RowTools extends TestUtils {

  val structWithColumnExpr = (rules: Int, cols: Int, df: DataFrame) =>
    df.withColumn("DataQuality", ruleRunner(genRules(rules, cols), compileEvals = false))

  val structWithColumnExprEvalCompiled = (rules: Int, cols: Int, df: DataFrame) =>
    df.withColumn("DataQuality", ruleRunner(genRules(rules, cols), compileEvals = true))

  val structWithColumnExprButRunnerEval = (rules: Int, cols: Int, df: DataFrame) =>
    df.withColumn("DataQuality", ruleRunner(genRules(rules, cols), compileEvals = false, forceRunnerEval = true))

  val structWithColumnExprEvalCompiledButRunnerEval = (rules: Int, cols: Int, df: DataFrame) =>
    df.withColumn("DataQuality", ruleRunner(genRules(rules, cols), compileEvals = true, forceRunnerEval = true))

  val noRules = (rules: Int, cols: Int, df: DataFrame) =>
    df.withColumn("DataQuality", lit("place"))

  val simplePassedProbabilityRule = RuleSuite(Id(1,1), Seq(
    RuleSet(Id(50, 1), Seq(
      Rule(Id(100, 1), ExpressionRule("0.9"))
    ))
  ))

  val simpleFailedProbabilityRule = RuleSuite(Id(1,1), Seq(
    RuleSet(Id(50, 1), Seq(
      Rule(Id(100, 1), ExpressionRule("0.6"))
    ))
  ))

  def genRules(rules: Int, cols: Int) =
    RuleSuite(Id(1, 1),
      for{
        r <- 2 to rules by 5
      } yield
        RuleSet(Id(50+r, 1),
          for {
            c <- 0 to cols / 2
          } yield Rule(Id(100+c, 1),
            ExpressionRule(s"(`$c` % 2) = 0")
          )
        )
    )

  import scala.collection.JavaConverters._

//  val fields = Gen.range("fieldCount")(27, 30, 5) // 1500 gives gc issues, takes forever to warm up, probably will soe
//  val ruleSets = Gen.range("rulesetCount")(27,30, 5) // number of rule sets run  2, 1)//
  val fields = Gen.range("fieldCount")(150, 150, 5) // 800 gives 1500 gives gc issues, takes forever to warm up, probably will soe
  val ruleSets = Gen.range("rulesetCount")(150,150, 5) // number of rule sets run  2, 1)//

  val generator = crossProduct(ruleSets, fields)

  val writeRows = 100000
  //val writeRows = 1000
  //val writeRows = 10000

  // data grows with the rows...
  def sampleDataAsLong[T](maxRows: Int, maxCols: Int, startValue: T): Seq[Row] =
    (0 to maxRows).map(i => (0L until maxCols).map(i+_)).map(t => Row((t :+ startValue) :_*))

  def dataFrameLong[T](maxRows: Int, maxCols: Int, dataType: DataType, startValue: T) = sparkSessionF.createDataFrame(sampleDataAsLong(maxRows, maxCols, startValue).asJava, longSchema(maxCols, dataType))

  def sampleDataAsLongLazy[T](ids: Dataset[java.lang.Long], maxCols: Int, startValue: T, structType: StructType): DataFrame = {
    implicit val renc = ShimUtils.rowEncoder(structType)
    ids.map(i => Row(((0L until maxCols).map(i+_) :+ startValue) :_*))
  }

  /**
   * unlike dataFrameLong it's lazy
   */
  def dataFrameLongLazy[T](maxRows: Int, maxCols: Int, dataType: DataType, startValue: T) = sampleDataAsLongLazy(sparkSessionF.range(0, maxRows), maxCols, startValue, longSchema(maxCols, dataType))

}

object WriteRowPerfTest extends Bench.OfflineReport with RowTools {

  performance of "resultWriting" config (
      exec.minWarmupRuns -> 2,
      exec.maxWarmupRuns -> 4,
      exec.benchRuns -> 4,
      exec.jvmcmd -> (System.getProperty("java.home")+"/bin/java"),
      exec.jvmflags -> List("-Xmx24g","-Xms24g")
      //  verbose -> true
    ) in {


    measure method "structWithColumnExprButRunnerEval" in {
      using(generator) in evaluate(structWithColumnExprButRunnerEval, StringType, null, "structWithColumnExprButRunnerEval")
    }

    /*
    measure method "noRules" in {
      using(generator) in evaluate(noRules, StringType, null, "noRules")
    }
      measure method "structColumnExprEvalsNotCompiled" in {
        using(generator) in evaluate(structWithColumnExpr, StringType, null, "structColumnExprEvalsNotCompiled")
      }
      measure method "structColumnExprEvalsCompiled" in {
        using(generator) in evaluate(structWithColumnExprEvalCompiled, StringType, null, "structColumnExprEvalsCompiled")
      }

      measure method "}structWithColumnExprEvalCompiledButRunnerEval" in {
        using(generator) in evaluate(structWithColumnExprEvalCompiledButRunnerEval, StringType, null, "structColumnExprEvalsCompiled")
      }*/
    /*measure method "structColumnExprEvalsNotCompiledDisk" in {
      using(generator) in evaluate(structWithColumnExpr, StringType, null, "structColumnExprEvalsNotCompiled", memory = false)
    }
    measure method "structColumnExprEvalsCompiledDisk" in {
      using(generator) in evaluate(structWithColumnExprEvalCompiled, StringType, null, "structColumnExprEvalsCompiled", memory = false)
    }*/
  }

  def evaluate(func: (Int, Int, DataFrame) => DataFrame, dataType: DataType, startValue: Any, testName: String,
               memory: Boolean = true)(params: (Int, Int)) = {
    val df = dataFrameLong(writeRows, params._2, dataType, startValue)
    val ndf = func(params._1, params._2, df)
    if (memory) {
      val sum =
      ndf.toLocalIterator().asScala.map { r => val i = r.fieldIndex("DataQuality"); r.get(i); r.getLong(1) }.sum // get will probably be dumped, but hopefully not
      sum
    } else {
      val path = SparkTestUtils.path("writePerfTest")
      ndf.write.mode("overwrite").parquet(path + testName) // force a write rather than gen then read - differently optimised
      ndf.count()
    }
  }

}

trait ReadBased extends RowTools {
  def runit(r: RuleSuiteResult) = r.ruleSetResults.size.toLong
  def rowUnit(r: Row) = r.getAs[Long]("0")

  def evaluateRead[T](f: DataFrame => Dataset[T], use: T => Long)(df: DataFrame) =
    f(df).toLocalIterator().asScala.map{ r =>
      val i = use(r)
      i
    }.sum

  val readRows = writeRows

  /**
    * Build the data outside of measurement
    */
  def readGen[T](func: (Int, Int, DataFrame) => DataFrame, dataType: DataType, startValue: T, inMemory: Boolean = true)  =
    for {
      field <- fields
      ruleSet <- ruleSets
    } yield {
      val df = dataFrameLong(readRows, ruleSet, dataType, startValue)
      val ndf = func(field, ruleSet, df)
      val res =
        if (inMemory) {
          val res = ndf.persist(StorageLevel.MEMORY_ONLY)
          res.rdd.count() // force it to evaluate, removes gen from the timings
          res
        } else {
          val path = SparkTestUtils.path("interimTest")
          ndf.write.mode("overwrite").parquet(path)
          sparkSessionF.read.parquet(path)
        }
      res
    }
}

object ReadRowPerfTest extends Bench.OfflineReport with ReadBased {

  val structRead = (df: DataFrame) => {
    import com.sparkutils.quality.impl.Encoders._
    df.select("DataQuality.*").as[RuleSuiteResult]
  }

  val justDataRead = (df: DataFrame) => {
    df.drop("DataQuality")
  }

  performance of "reading" config (
    exec.minWarmupRuns -> 2,
    exec.maxWarmupRuns -> 4,
    exec.benchRuns -> 4,
    exec.jvmcmd -> (System.getProperty("java.home")+"/bin/java"),
    exec.jvmflags -> List("-Xmx8g","-Xms8g")
    //  verbose -> true
  ) in {

    measure method "readColumnStruct" in {
      using(readGen[RuleSuiteResult](structWithColumnExpr, ruleSuiteResultType, null)) in evaluateRead(structRead, runit)
    }

    measure method "readJustData" in {
      using(readGen[RuleSuiteResult](structWithColumnExpr, ruleSuiteResultType, null)) in evaluateRead(justDataRead, rowUnit)
    }
  }
}

object FilterRowPerfTest extends Bench.OfflineReport with ReadBased {

  /*
  works but only overall for struct
  ds.filter("res.overallResult.resultType = 'Failed'").show()

  ds.select("res.ruleSetResults.`Test Set5`.overallResult").head

  specific works but nested "having" or xpath[] doesn't seem to have a syntax, and wildcard doesn't
  ds.filter("res.ruleSetResults.`Test Set5`.ruleResults.`Rule 1`.resultType = 'Probability'").head

for structs.
// filter and map values works but then need filtering
ds.select(col("*"), expr("filter(map_values(res.ruleSetResults), ruleSet -> size(filter(map_values(ruleSet.ruleResults), rule -> rule.resultType = 'Probability' and double(rule.parameters.`percentage`) > 0.3)) > 0)").as("filtered")).filter("size(filtered) > 0").show()
   */

  val structFilterOverall = (df: DataFrame) => {
    df.filter("DataQuality.overallResult = passed()")
  }

  val structFilterNested = (df: DataFrame) => {
    df.select(col("*"), expr("filter(map_values(DataQuality.ruleSetResults), ruleSet -> size(filter(map_values(ruleSet.ruleResults), result -> probability(result) > 0.3)) > 0)").as("filtered")).filter("size(filtered) > 0")
  }

  val unit = rowUnit _

  override val readRows = writeRows * 10

  performance of "filtering" config (
    exec.minWarmupRuns -> 2,
    exec.maxWarmupRuns -> 4,
    exec.benchRuns -> 4,
    exec.jvmcmd -> (System.getProperty("java.home")+"/bin/java"),
    exec.jvmflags -> List("-Xmx8g","-Xms8g")
    //  verbose -> true
  ) in {
    measure method "filterColumnStructOverall" in {
      using(readGen[RuleSuiteResult](structWithColumnExpr, ruleSuiteResultType, null, false)) in evaluateRead(structFilterOverall, unit)
    }

    measure method "filterColumnStructNested" in {
      using(readGen[RuleSuiteResult](structWithColumnExpr, ruleSuiteResultType, null, false)) in evaluateRead(structFilterNested, unit)
    }

  }
}

/**
 * Not DQ related but a convenient place to dump this
 */
object UUIDPerfTest extends Bench.OfflineReport with RowTools {
  val rowCount = 1000000
    // writeRows * 3

  val rows = Gen.range("fieldCount")(rowCount, rowCount, 1)

  performance of "UUIDWriting" in {
    measure method "writeWithPCG" in {
      using(rows) in evaluate(_.withColumn("uuid", rng_bytes(RandomSource.PCG_RXS_M_XS_64) ), "writeWithPCG")
    }
    measure method "writeWithXOR128" in {
      using(rows) in evaluate(_.withColumn("uuid", RandomBytes(RandomSource.XO_RO_SHI_RO_128_PP) ), "writeWithXOR")
    }
    measure method "writeWithoutUUID" in {
      using(rows) in evaluate(_.withColumn("uuid", col("id")), "writeWithoutUUID")
    }
    measure method "writeWithUuid" in {
      using(rows) in evaluate(_.withColumn("uuid", expr("uuid()")), "writeWithUuid")
    }
    measure method "writeWithMonotonicallyIncrementing" in {
      using(rows) in evaluate(_.withColumn("uuid", expr("monotonically_increasing_id()")), "writeWithMonotonicallyIncrementing")
    }
  }

  def evaluate(func: Dataset[java.lang.Long] => DataFrame, testName: String)(params: Int) = {
    val rdf = sparkSessionF.range(0, params)
    val df = func(rdf)
    val path = SparkTestUtils.path("writePerfTest")
    //df.write.mode("overwrite").parquet(path + testName) // force a write rather than gen then read - differently optimised
    //df.count()

    df.toLocalIterator().asScala.map(_.getLong(0)).sum
  }


}

case class RowId(lower: Long, higher: Long)

// Seperate app as the data takes 1h+ to generate.  The tests alone will run to 20m or so each.
// D4s3 10000000 takes 8minutes which is mostly enough
object IDJoinStarter {
  val maxRows = 10000000//00

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().config("spark.master", "local").getOrCreate()

    import spark.implicits._

    // 128 bit

    val id = (s: Long) => {
      RowId(s, s+1)
    }
    spark.udf.register("lId", id)

    val bid = (s: Long) => {
      val array = Array.ofDim[Byte](16)
      array(7) = (s >> 56).asInstanceOf[Byte]
      array(6) = (s >> 48).asInstanceOf[Byte]
      array(5) = (s >> 40).asInstanceOf[Byte]
      array(4) = (s >> 32).asInstanceOf[Byte]
      array(3) = (s >> 24).asInstanceOf[Byte]
      array(2) = (s >> 16).asInstanceOf[Byte]
      array(1) = (s >> 8).asInstanceOf[Byte]
      array(0) = (s).asInstanceOf[Byte]

      val s1 = (s + 1)
      array(15) = (s1 >> 56).asInstanceOf[Byte]
      array(14) = (s1 >> 48).asInstanceOf[Byte]
      array(13) = (s1 >> 40).asInstanceOf[Byte]
      array(12) = (s1 >> 32).asInstanceOf[Byte]
      array(11) = (s1 >> 24).asInstanceOf[Byte]
      array(10) = (s1 >> 16).asInstanceOf[Byte]
      array(9) = (s1 >> 8).asInstanceOf[Byte]
      array(8) = (s1).asInstanceOf[Byte]

      array
    }
    spark.udf.register("bId", bid)

    val idsRaw = spark.range(0, maxRows).selectExpr("id", "uuid() as uuid", "lId(id) as nestedLongs", "bId(id) as bytes", "id as lower", "(id + 1) as higher").repartitionByRange(numPartitions = 200, $"id")
    idsRaw.write.mode("overwrite").parquet(SparkTestUtils.path("ids_for_ctw_joins"))
    val ids = spark.read.parquet(SparkTestUtils.path("ds_for_ctw_joins"))
    //idsRaw.persist(org.apache.spark.storage.StorageLevel.DISK_ONLY) // force persist to disk

    val evensRaw = ids.filter("id % 2 = 0").selectExpr("uuid as euuid", "nestedLongs as enestedLongs", "bytes as ebytes", "lower as elower", "higher as ehigher", "id as eid").repartitionByRange(numPartitions = 200, $"eid")
    evensRaw.write.mode("overwrite").parquet(SparkTestUtils.path("evens_for_ctw_joins"))
    val evens = spark.read.parquet(SparkTestUtils.path("evens_for_ctw_joins"))
    // evensRaw.persist(org.apache.spark.storage.StorageLevel.DISK_ONLY) // force persist to disk

  }
}

/**
 * Not DQ related but a convenient place to dump this
 */
object IDJoinPerfTest extends Bench.OfflineReport with RowTools {
  val rowCount = IDJoinStarter.maxRows
  // writeRows * 3
  val stable = sparkSessionF
  import stable.implicits._

  val rows = Gen.range("rowCount")(rowCount, rowCount, 1)

  performance of "IDJoins" in {
    measure method "joinWithSingleLong" in {
      using(rows) in evaluate{ (ids, evens) => evens.join(ids, $"eid" === $"id") }
    }
    measure method "joinWithNestedLongs" in {
      using(rows) in evaluate( (ids, evens) => evens.join(ids, $"enestedLongs" === $"nestedLongs") )
    }
    measure method "joinWithBytes" in {
      using(rows) in evaluate( (ids, evens) => evens.join(ids, $"eBytes" === $"bytes") )
    }
    measure method "joinWithTwoLongs" in {
      using(rows) in evaluate( (ids, evens) => evens.join(ids, $"eLower" === $"lower" && $"eHigher" === $"higher") )
    }
    measure method "joinWithUUIDString" in {
      using(rows) in evaluate( (ids, evens) => evens.join(ids, $"eUuid" === $"uuid") )
    }
  }

  def evaluate(func: (DataFrame, DataFrame) => DataFrame)(params: Int) = {
    val ids = sparkSessionF.read.parquet(SparkTestUtils.path("ids_for_ctw_joins"))
    //idsRaw.persist(org.apache.spark.storage.StorageLevel.DISK_ONLY) // force persist to disk

    val evens = sparkSessionF.read.parquet(SparkTestUtils.path("evens_for_ctw_joins"))
    // evensRaw.persist(org.apache.spark.storage.StorageLevel.DISK_ONLY) // force persist to disk

    val df = func(ids, evens)

    // enough to force for file store
    df.count
  }


}

/**
 * Looks at difference between compiled and non-compiled lambdas using generation of
 * lambdas doing simple increments which should be simple enough thing to optimise
 */
object LambdaRowPerfTest extends Bench.OfflineReport with RowTools {
  val ruleCount = Gen.range("ruleCount")(2, 30, 5) // 1500 gives gc issues, takes forever to warm up, probably will soe

  val cols = 20

  val genLambdas = (setup: () => Unit) => (rules: Int, df: DataFrame) => {
    setup()
    df.withColumn("DataQuality", ruleFolderRunner(
      mapRules(genRules(rules, cols)) {
        rule =>
          rule.copy(runOnPassProcessor = RunOnPassProcessor(
            1000, Id(rule.id.id + 10000, rule.id.version + 10000),
            OutputExpression("thecurrent -> updateField(thecurrent, 'thecount', thecurrent.thecount + 1)")
          ))
      }, struct(lit(0).as("thecount"))))
  }

  val structWithLambdaDisabled = genLambdas(() => System.setProperty("quality.lambdaHandlers","org.apache.spark.sql.qualityFunctions.FunN=org.apache.spark.sql.qualityFunctions.DoCodegenFallbackHandler"))

  val structWithLambdaCompilationEnabled = genLambdas(() => System.clearProperty("quality.lambdaHandlers"))

  performance of "lambdaCompilation" config (
    exec.minWarmupRuns -> 2,
    exec.maxWarmupRuns -> 4,
    exec.benchRuns -> 4,
    exec.jvmcmd -> (System.getProperty("java.home")+"/bin/java"),
    exec.jvmflags -> List("-Xmx8g","-Xms8g")
    //  verbose -> true
  ) in {
    measure method "lambdaNotCompiled" in {
      using(ruleCount) in evaluate(structWithLambdaDisabled, StringType, null, "structColumnExprEvalsNotCompiled")
    }
    measure method "lambdaCompiled" in {
      using(ruleCount) in evaluate(structWithLambdaCompilationEnabled, StringType, null, "structColumnExprEvalsCompiled")
    }/*
    measure method "structColumnExprEvalsNotCompiledDisk" in {
      using(generator) in evaluate(structWithColumnExpr, StringType, null, "structColumnExprEvalsNotCompiled", memory = false)
    }
    measure method "structColumnExprEvalsCompiledDisk" in {
      using(generator) in evaluate(structWithColumnExprEvalCompiled, StringType, null, "structColumnExprEvalsCompiled", memory = false)
    } */
  }

  def evaluate(func: (Int, DataFrame) => DataFrame, dataType: DataType, startValue: Any, testName: String,
               memory: Boolean = true)(params: (Int)) = {
    registerQualityFunctions()
    val df = dataFrameLong(writeRows, cols, dataType, startValue)
    val ndf = func(params, df)
    if (memory) {
      val sum =
        ndf.toLocalIterator().asScala.map { r => val i = r.fieldIndex("DataQuality"); r.get(i); r.getLong(1) }.sum // get will probably be dumped, but hopefully not
      sum
    } else {
      val path = SparkTestUtils.path("writePerfTest")
      ndf.write.mode("overwrite").parquet(path + testName) // force a write rather than gen then read - differently optimised
      ndf.count()
    }
  }

}
