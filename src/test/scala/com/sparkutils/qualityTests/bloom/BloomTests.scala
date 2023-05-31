package com.sparkutils.qualityTests.bloom

import com.sparkutils.quality._
import com.sparkutils.qualityTests._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.junit.Test
import org.scalatest.FunSuite

case class Pair(a: Long, b: Long)

class BloomTests extends FunSuite with TestUtils {

  def directCreateSpark() = {
    // train it
    val df = sqlContext.range(1, 20)
    val bloom = df.stat.bloomFilter("id", 20, 0.01)
    val fpp = 1.0 - bloom.expectedFpp()
    val blooms: BloomFilterMap = Map("ids" -> (SparkBloomFilter(bloom), fpp))
    (blooms, fpp)
  }

  def directCreateParquet() =
    directCreateParquetI(bloomLookup(_), "smallBloom")

  def directCreateBucketedArrayParquet(bloomType: String = "eager") = {
    registerQualityFunctions()
    // train it
    val orig = sqlContext.range(1, 20)

    import com.sparkutils.quality.impl.bloom.parquet.ThreadSafeBucketedBloomLookup
/*
    val interim = orig.select(expr(s"bigBucketedParquetBloom(id, 20, 0.01, 'superBloom') as bloom")).head.getAs[Array[Byte]](0)//.select("bloom.*")
    //val bloom = QualitySparkUtils.obtainBroadcast[BroadcastedBloom](interim)
    val bloom = BucketedFiles.deserialize(interim)
    bloom.cleanupOthers()
    */
    val bloom = bloomFrom(orig, "id", 20)

    val bf =
      bloomType match {
        case "eager" => ThreadSafeBucketedBloomLookup(bloom)
        case "lazy" => ThreadSafeBucketedBloomLookup.lazyLookup(bloom)
        case "mapped" => ThreadSafeBucketedBloomLookup.mappedLookup(bloom)
      }
    val blooms: BloomFilterMap = Map("ids" -> (bf, bloom.fpp))
    (blooms, bloom.fpp)
  }

  def directCreateParquetI(lookupImpl: Array[Byte] => BloomLookup, bloomF: String) = {
    registerQualityFunctions()
    // train it
    val orig = sqlContext.range(1, 20)
    val aggrow = orig.select(expr(s"$bloomF(id, 20, cast(0.01 as double))")).head()
    val thebytes = aggrow.getAs[Array[Byte]](0)
    val bf = lookupImpl(thebytes)
    val fpp = 0.99
    val blooms: BloomFilterMap = Map("ids" -> (bf, fpp))
    (blooms, fpp)
  }

  def doVerifyMeasurement(func: (BloomFilterMap, DataFrame) => DataFrame, bloomsF: () => (BloomFilterMap, Double) = directCreateSpark): Unit = evalCodeGensNoResolve { // eval only works here - can't compile due to types being messsed up?
    val (blooms, fpp) = bloomsF()

    import sqlContext.implicits._

    val comporig = sqlContext.createDataset(Seq(
      Pair(0, 0),
      Pair(1, 0),
      Pair(0, 4),
      Pair(2, 2),
      Pair(2, 3),
      Pair(0, 30),
      Pair(2, 10),
      Pair(20, -4),
      Pair(16, 0),
      Pair(0, 14),
      Pair(7, 1),
      Pair(220, 0),
      Pair(0, 3430)
    )).toDF()
    val compdf = func(blooms, comporig)
    compdf.show(30)

    assert(4 == compdf.filter("probability = 0.0").count(), "Should have only 3 rows not finding a match")
    assert(9 == compdf.filter(s"probability = $fpp").count(), s"Should have had 9 rows with $fpp, either a rounding error or borked")
  }

  def doVerifyMeasurementColumn(bloomsF: () => (BloomFilterMap, Double)): Unit =
    doVerifyMeasurement((bloomFilterMap, df) => {
      val bloomMap = SparkSession.getActiveSession.get.sparkContext.broadcast(bloomFilterMap)

      df.withColumn("probability", bloomFilterLookup(col("a") + col("b"), lit("ids"), bloomMap))},
      bloomsF)

  def doVerifyMeasurementSQL(bloomsF: () => (BloomFilterMap, Double)): Unit =
    doVerifyMeasurement((bloomFilterMap, df) => {
      val bloomMap = SparkSession.active.sparkContext.broadcast(bloomFilterMap)
      registerBloomMapAndFunction(bloomMap)
        df.withColumn("probability", expr("probabilityIn(a + b, 'ids')"))},
    bloomsF)

  @Test
  def verifyMeasurementColumnSpark(): Unit = evalCodeGensNoResolve {
    doVerifyMeasurementColumn(directCreateSpark)
  }

  @Test
  def verifyMeasurementSQLSpark(): Unit = evalCodeGensNoResolve {
    doVerifyMeasurementSQL(directCreateSpark)
  }

  @Test
  def verifyMeasurementColumnParquet(): Unit = evalCodeGensNoResolve {
    doVerifyMeasurementColumn(directCreateParquet)
  }

  @Test
  def verifyMeasurementSQLParquet(): Unit = evalCodeGensNoResolve {
    doVerifyMeasurementSQL(directCreateParquet)
  }

  def createViaDFRoundTripSpark() = {
    // train it
    val (blooms, fpp) = directCreateSpark()

    import com.sparkutils.quality.impl.bloom.Serializing.sparkBloomFilterSerializer

    val ds = com.sparkutils.quality.impl.bloom.Serializing.toDF(blooms)
    val blooms2 = com.sparkutils.quality.impl.bloom.Serializing.fromDF(ds.toDF(), col("bloom_id"), col("bloom"))
    (blooms2, fpp)
  }

  def doVerifyMeasurementSQLRoundTrip(f: () => (BloomFilterMap, Double)): Unit =
    doVerifyMeasurement({(bloomFilterMap, df) =>
      val bloomMap = SparkSession.getActiveSession.get.sparkContext.broadcast(bloomFilterMap)
      registerBloomMapAndFunction(bloomMap)
      df.withColumn("probability", expr("probabilityIn(a + b, 'ids')"))
    }, f)

  def doVerifyCompilation(f: () => (BloomFilterMap, Double)): Unit =
    doVerifyMeasurement({(bloomFilterMap, df) =>
      val bloomMap = SparkSession.getActiveSession.get.sparkContext.broadcast(bloomFilterMap)
      registerBloomMapAndFunction(bloomMap)

      val fname = outputDir + "/bloomingMarvelous"

      df.write.mode(SaveMode.Overwrite).parquet(fname)

      val reread = df.sparkSession.read.parquet(fname)

      reread.withColumn("probability", expr("probabilityIn(a + b, 'ids')"))
    }, f)

  @Test
  def verifyMeasurementSQLRoundTripSpark(): Unit =
    doVerifyMeasurementSQLRoundTrip(createViaDFRoundTripSpark)

  @Test
  def verifyCompilationSpark(): Unit =
    doVerifyCompilation(createViaDFRoundTripSpark)

  @Test
  def verifyMeasurementSQLRoundTripBucketedArrayEager(): Unit =
    doVerifyMeasurementSQLRoundTrip(() => directCreateBucketedArrayParquet("eager"))

  @Test
  def verifyCompilationBucketedArrayEager(): Unit =
    doVerifyCompilation(() => directCreateBucketedArrayParquet("eager"))

  @Test
  def verifyMeasurementSQLRoundTripBucketedArrayLazy(): Unit =
    doVerifyMeasurementSQLRoundTrip(() => directCreateBucketedArrayParquet("lazy"))

  @Test
  def verifyCompilationBucketedArrayLazy(): Unit =
    doVerifyCompilation( () => directCreateBucketedArrayParquet("lazy") )

  @Test
  def verifyMeasurementSQLRoundTripBucketedArrayMapped(): Unit =
    doVerifyMeasurementSQLRoundTrip( () => directCreateBucketedArrayParquet("mapped") )

  @Test
  def verifyCompilationBucketedArrayMapped(): Unit =
    doVerifyCompilation(() => directCreateBucketedArrayParquet("mapped"))

  @Test
  def assertIncrementalBucketsViaFPP(): Unit = evalCodeGensNoResolve {
    // going beyond 10 is pointless for a bloom and will cause a wrap around
    val numItems = 10000000000L
    var prev = 0L
    for{ i <- 1 to 10} {
      val fpp = 1d / (0 until i).foldLeft(1){ (p, a) => p * 10}.toDouble
      val buckets = optimalNumberOfBuckets(numItems, fpp)
      assert(buckets > prev, "As we increase FPP we should see an increased number of buckets" )
      prev = buckets
    }
  }

  @Test
  def assertIncrementalBucketsViaExpectedNums(): Unit = evalCodeGensNoResolve {
    val fpp = 0.01
    val numItemsStarter = 1000000000000L
    var prev = 0L
    for{ i <- numItemsStarter to (15*numItemsStarter) by numItemsStarter} {
      val buckets = optimalNumberOfBuckets(i, fpp)
      assert(buckets > prev, "As we increase FPP we should see an increased number of buckets" )
      prev = buckets
    }
  }

  @Test
  def verifyBloomsAreIdentified(): Unit = evalCodeGensNoResolve {
     val rules = RuleSuite(Id(1,1), Seq(
      RuleSet(Id(50, 1), Seq(
        Rule(Id(100, 1), ExpressionRule("1<2 and probabilityIn(a + b, 'ids')")),
        Rule(Id(100, 2), ExpressionRule("1")),
        Rule(Id(100, 3), ExpressionRule("3")),
        Rule(Id(100, 4), ExpressionRule("4"))
      )),
      RuleSet(Id(50, 2), Seq(
        Rule(Id(100, 5), ExpressionRule("probabilityIn(a + b, 'dont have it') > 1304")),
        Rule(Id(100, 6), ExpressionRule("5")),
        Rule(Id(100, 7), ExpressionRule("probabilityIn(a + b, 'amy')")),
        Rule(Id(100, 8), ExpressionRule("6"))
      )),
      RuleSet(Id(50, 3), Seq(
        Rule(Id(100, 9), ExpressionRule("10")),
        Rule(Id(100, 10), ExpressionRule("probabilityIn(a + b, 'bungle') and (1 + 2) = 3")),
        Rule(Id(100, 11), ExpressionRule("7")),
        Rule(Id(100, 12), ExpressionRule("rowid(rng(), 'other')"))
      ))
    ))

    val ids = getBlooms(rules)
    ids.foreach(println)

    val expected = Seq("ids", "dont have it", "amy", "bungle", "other")
    assert(expected == ids, "Did not get the expected id's in the right order")
  }

  @Test
  def verifyInputParams(): Unit = evalCodeGensNoResolve {
    registerQualityFunctions()
    // train it
    val orig = sqlContext.range(1, 20)

    try {
      val interim = orig.select(expr(s"bigBloom(id, 'a', cast(1 as double),'superBloom') as bloom")).head.getAs[Array[Byte]](0)
      fail("should not have got here a not valid")
    } catch {
      case t: Throwable =>
        val spark2_and_3 = t.getMessage.contains("Should have been Short, Long or Integer but was: a UTF8String")
        val spark32 = t.getMessage.contains(" however, ''a'' is of string type")
        // orig spark 3.4 rc1'ish and dbr 11/12
        val early_spark34_dbr11_n_12 = t.getMessage.contains(" however, \"a\" is of \"STRING\" type")
        val spark34 = t.getMessage.contains("however \"a\" has the type \"STRING\".")
        assert(spark2_and_3 || spark32 || spark34 || early_spark34_dbr11_n_12)
    }
    try {
      val interim = orig.select(expr(s"bigBloom(id, 1, 1,'superBloom') as bloom")).head.getAs[Array[Byte]](0)
      fail("should not have got here a not valid")
    } catch {
      case t: Throwable =>
        val spark2_and_3 = t.getMessage.contains("Should have been Double or Decimal but was: 1 Integer")
        val spark32 = t.getMessage.contains(" however, '1' is of int type")
        // orig spark 3.4 rc1'ish and dbr 11/12
        val early_spark34_dbr11_n_12 = t.getMessage.contains(" however, \"1\" is of \"INT\" type")
        val spark34 = t.getMessage.contains("however \"1\" has the type \"INT\".")
        assert(spark2_and_3 || spark32 || spark34 || early_spark34_dbr11_n_12)
    }
  }

  @Test
  def shouldThrowOnUnknownBlooms(): Unit = {
    val b = sparkSession.sparkContext.broadcast(Map.empty[ String, (BloomLookup, Double) ])
    registerBloomMapAndFunction(b)
    try {
      val aggrow = sparkSession.sql(s"select probabilityIn(1234, 'non')").head()
      fail("Should have thrown on lookup")
    } catch {
      case t: Throwable => assert(anyCauseHas(t, {
        case q: QualityException if q.getMessage.contains("non, does not exist") => true
        case _ => false
      }), "didn't have non")
    } finally {
      b.unpersist()
    }
  }

}
