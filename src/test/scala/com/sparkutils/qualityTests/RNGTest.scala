/*package com.sparkutils.qualityTests

import com.sparkutils.quality.spark.bloom.parquet.{ThreadSafeBloomLookupImpl, ThreadSafeBucketedByteArrayArrayBloomLookupImpl, ThreadSafeBucketedByteArrayBloomLookupImpl}
import com.sparkutils.quality.spark.bloom.{BloomFilterLookup, BloomFilterTypes, BloomStruct, ParquetBloomFilter}
import com.sparkutils.quality.spark.rowid.RowID
import com.sparkutils.quality.{RowTools, TestUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.expr
import org.junit.Test
import org.scalatest.FunSuite

/**
 * Needs a big box for this
 */
class RNGTest extends FunSuite with RowTools with TestUtils {

  type Bytes = Array[Byte]

  @Test
  def rowidBytesParquetTest: Unit = { // evalCodeGens for the big rows we need to let it do it's thing
    val numRows = 10000//00000
    import com.sparkutils.quality.spark.bloom.parquet._

    val start = System.currentTimeMillis()

    def getRNGStarter() = {
      val ids = sparkSession.range(numRows)
      ids.selectExpr("*", "rngBytes() as uuid")
    }

    def getRNGS() = {
      val ids = sparkSession.range(numRows)
      ids.selectExpr("*", "rowid(rngBytes(), 'ids') as uuid")
    }

    val orig = getRNGStarter()

    val bloomGenStart = System.currentTimeMillis()

    val aggrow = orig.select(expr(s"bucketedParquetBloom(uuid, $numRows, 0.01)")).head()
    val thebytes = aggrow.getAs[Bytes](0)
    var bf = BucketedCreator(thebytes)
    val bfl = ThreadSafeBucketedByteArrayBloomLookupImpl(thebytes)
    val fpp = 0.99
    val blooms: BloomFilterTypes.BloomFilterMap = Map("ids" -> (ParquetBloomFilter(bfl), fpp))

    val bloomMap = SparkSession.getActiveSession.get.sparkContext.broadcast(blooms)

    BloomFilterLookup.registerFunction(bloomMap)
    RowID.registerFunction(blooms)

    val (bdiff, bunit) = stop(bloomGenStart)
    println(s"generated bloom map of $numRows entries in $bdiff $bunit")

    var countFalsePositive = 0L
    var count = 0L

    for{ i <- 0 until 10 } { // doing 11 breaks sprk /app summit
      val starti = System.currentTimeMillis()
      val withRNG = getRNGS()
      val matches = withRNG.withColumn("probability", expr("probabilityIn(uuid, 'ids')")).
        filter("probability > 0")
      //matches.show()

      val matchesCount = matches.count()
      countFalsePositive += matchesCount

      val (cidiff, ciunit) = stop(starti)
      println(s"before actual count $i $matchesCount ($countFalsePositive) false positives took $cidiff $ciunit")
      val (collisionsCount, startc) =
        if (matchesCount != 0) {
          // inner join
          val collisions = matches.join(orig, "uuid")
          //collisions.show()

          val startc = System.currentTimeMillis()

          val collisionsCount = collisions.filter("isNotNull(uuid)").count()
          count += collisionsCount
          (collisionsCount, startc)
        } else
          (0, System.currentTimeMillis())


      val (idiff, iunit) = stop(starti)
      val (icdiff, icunit) = stop(startc)

      val startb = System.currentTimeMillis()
      // get new ones
      val aggrow = withRNG.select(expr(s"bucketedParquetBloom(uuid, $numRows, 0.01)")).head()
      val thenewbytes = aggrow.getAs[Bytes](0)

      val fpp = 0.99

      val newbf = BucketedCreator(thenewbytes)
      bf |= newbf
      val newbfl = ThreadSafeBucketedByteArrayBloomLookupImpl(bf.serialized)

      val blooms: BloomFilterTypes.BloomFilterMap = Map("ids" -> (ParquetBloomFilter(newbfl), fpp))
      val bloomMap = SparkSession.getActiveSession.get.sparkContext.broadcast(blooms)

      // overwrite it for the next round
      BloomFilterLookup.registerFunction(bloomMap)
      RowID.registerFunction(blooms)
      val (bdiff, bunit) = stop(startb)


      // count += //matches.filter("probability > 0").count()
      println(s"ran iteration $i added $matchesCount ($countFalsePositive) false positives, $collisionsCount ($count) collisions in $idiff $iunit total, and $icdiff $icunit just for join and count.  Adding to the bloom took $bdiff $bunit")
    }

    val (diff, unit) = stop(start)

    println(s"took $diff $unit - for ${countFalsePositive} non false rows out of ${numRows * 11} total")
    assert( 0 == count, s"Should have had 0 collisions and not ever run evaluations instead had ${count} per ${numRows * 11} collisions")
    assert( 0 == countFalsePositive, s"Should have had 0 original false positives instead had ${countFalsePositive} per ${numRows * 11} collisions")
  }

  @Test
  def rowidLongParquetTest: Unit = forceInterpreted { // evalCodeGens for the big rows we need to let it do it's thing
    val numRows = 10000//00000
    import com.sparkutils.quality.spark.bloom.parquet._

    val start = System.currentTimeMillis()

    def getRNGStarter() = {
      val ids = sparkSession.range(numRows)
      ids.selectExpr("*", "rng() as nested").selectExpr("*", "nested.*")
    }

    def getRNGS() = {
      val ids = sparkSession.range(numRows)
      ids.selectExpr("*", "rowid(rng(), 'ids') as nested").selectExpr("*", "nested.*")
    }

    val orig = getRNGStarter()

    val bloomGenStart = System.currentTimeMillis()

    val aggrow = orig.select(expr(s"bucketedParquetBloom(id(lower, higher), $numRows, 0.01)")).head()
    val thebytes = aggrow.getAs[Bytes](0)
    var bf = BucketedCreator(thebytes)
    val bfl = ThreadSafeBucketedByteArrayBloomLookupImpl(thebytes)
    val fpp = 0.99
    val blooms: BloomFilterTypes.BloomFilterMap = Map("ids" -> (ParquetBloomFilter(bfl), fpp))
    val bloomMap = SparkSession.getActiveSession.get.sparkContext.broadcast(blooms)

    BloomFilterLookup.registerFunction(bloomMap)
    RowID.registerFunction(blooms)

    val (bdiff, bunit) = stop(bloomGenStart)
    println(s"generated bloom map of $numRows entries in $bdiff $bunit")

    var countFalsePositive = 0L
    var count = 0L

    for{ i <- 0 until 10 } { // doing 11 breaks sprk /app summit
      val starti = System.currentTimeMillis()
      val withRNG = getRNGS()
      val matches = withRNG.withColumn("probability", expr("probabilityIn(nested, 'ids')")).
        filter("probability > 0")
      //matches.show()

      val matchesCount = matches.count()
      countFalsePositive += matchesCount

      val (cidiff, ciunit) = stop(starti)
      println(s"before actual count $i $matchesCount ($countFalsePositive) false positives took $cidiff $ciunit")
      val (collisionsCount, startc) =
        if (matchesCount != 0) {
          // inner join
          val collisions = matches.join(orig, "uuid")
          //collisions.show()

          val startc = System.currentTimeMillis()

          val collisionsCount = collisions.filter("isNotNull(uuid)").count()
          count += collisionsCount
          (collisionsCount, startc)
        } else
          (0, System.currentTimeMillis())


      val (idiff, iunit) = stop(starti)
      val (icdiff, icunit) = stop(startc)

      val startb = System.currentTimeMillis()
      // get new ones
      val aggrow = withRNG.select(expr(s"bucketedParquetBloom(id(lower, higher), $numRows, 0.01)")).head()
      val thenewbytes = aggrow.getAs[Bytes](0)

      val fpp = 0.99

      val newbf = BucketedCreator(thenewbytes)
      bf |= newbf
      val newbfl = ThreadSafeBucketedByteArrayBloomLookupImpl(bf.serialized)

      val blooms: BloomFilterTypes.BloomFilterMap = Map("ids" -> (ParquetBloomFilter(newbfl), fpp))
      val bloomMap = SparkSession.getActiveSession.get.sparkContext.broadcast(blooms)

      // overwrite it for the next round
      BloomFilterLookup.registerFunction(bloomMap)
      RowID.registerFunction(blooms)
      val (bdiff, bunit) = stop(startb)


      // count += //matches.filter("probability > 0").count()
      println(s"ran iteration $i added $matchesCount ($countFalsePositive) false positives, $collisionsCount ($count) collisions in $idiff $iunit total, and $icdiff $icunit just for join and count.  Adding to the bloom took $bdiff $bunit")
    }

    val (diff, unit) = stop(start)

    println(s"took $diff $unit - for ${countFalsePositive} non false rows out of ${numRows * 11} total")
    assert( 0 == count, s"Should have had 0 collisions and not ever run evaluations instead had ${count} per ${numRows * 11} collisions")
    assert( 0 == countFalsePositive, s"Should have had 0 original false positives instead had ${countFalsePositive} per ${numRows * 11} collisions")
  }


  def fromBytes( lookupImpl: Array[Byte] => com.sparkutils.quality.spark.bloom.parquet.BloomLookup ) =
    (df: DataFrame) => {
      val aggrow = df.head()

      val thebytes = aggrow.getAs[Bytes](0)
      val bf = lookupImpl(thebytes)
      bf
    }

  @Test
  def blockParquetTest: Unit = {
    val numRows = 10000000//00
    doParquetTest(numRows, bloomAggr = "parquetBloom", fromBytes( ThreadSafeBloomLookupImpl(_)) )
  }
// took 2 m -   for 4 non false rows out of 110000000 total
  // with "correct" bytes to bits / 16 0.01 took 2 m - for 63529 non false rows out of 110000000 total
  // took 3 m 0.01 - for 1728 non false rows out of 110000000 total
  // generated bloom map of 1000000000 entries in 10 m
  //before actual count 0    525754959 (525754959) false positives took 10 m
  @Test
  def blockParquetBucketedTest: Unit = { //  -Dspark.driver.maxResultSize=4g and 25gb needed for a single host, 4g either way needed probably
    val numRows = 1000000000
    doParquetTest(numRows, bloomAggr = "bucketedParquetBloom", fromBytes(ThreadSafeBucketedByteArrayBloomLookupImpl(_)))
  }

  @Test
  def blockParquetBucketedArrayTest: Unit = { //  -Dspark.driver.maxResultSize=4g -Xmx25g 25gb needed for a single host, 4g either way needed probably
    val numRows = 1000000000
    import BloomStruct._

    doParquetTest(numRows, bloomAggr = "bigBucketedParquetBloom", df => {
      val aggrow = df.select("bloom.*").repartition(20).as[BloomStruct].head
      val bf = ThreadSafeBucketedByteArrayArrayBloomLookupImpl(aggrow.blooms, aggrow.fpp)
      bf
    }, "0.00001") // crazy high to see impact
  }


  def doParquetTest(numRows: Long, bloomAggr: String, lookupImpl: DataFrame => com.sparkutils.quality.spark.bloom.parquet.BloomLookup, sfpp: String = "0.001"): Unit = { // evalCodeGens for the big rows we need to let it do it's thing
    // 100000000 ooms

    // wow - got to test actually running the original through
    // took 17375 ms - for 1 non false rows out of  11000000 total - holy sh1t
    // took 3 m      - for 15 non false rows out of 110000000 total
    // took 26 m     - for 10047060 non false rows out of 1100000000 total - still
    /*
    generated bloom map of 100000000 entries in 1 m
    before actual count 0     913564 (913564) false positives took 51874 ms  so 10% better than the others and quite a bit faster
     */
    /*
    generated bloom map of 1000000 entries in 3078 ms
    before actual count 0 982291 (982291) false positives took 1501 ms
     */
    //val bf = BloomFilter.optimallySized[Array[Byte]](numRows, 0.01)

    val start = System.currentTimeMillis()

    def getRNGS() = {
      val ids = sparkSession.range(numRows)
      ids.selectExpr("*", "rngBytes() as uuid")
    }

    // attempt to speed this up a bit - repartition and caching fails marvellously turning a 14m per billion into 2 hours of fun
    val orig = getRNGS()

    val bloomGenStart = System.currentTimeMillis()

    val bf = lookupImpl( orig.select(expr(s"$bloomAggr(uuid, $numRows, $sfpp) as bloom")) )

    val fpp = 0.999
    val blooms: BloomFilterTypes.BloomFilterMap = Map("ids" -> (ParquetBloomFilter(bf), fpp))
    val bloomMap = SparkSession.getActiveSession.get.sparkContext.broadcast(blooms)

    BloomFilterLookup.registerFunction(bloomMap)

    val (bdiff, bunit) = stop(bloomGenStart)
    println(s"generated bloom map of $numRows entries in $bdiff $bunit")

    var countFalsePositive = 0L
    var count = 0L

    for{ i <- 0 to 10 } {
      val starti = System.currentTimeMillis()
      val withRNG = getRNGS()
      val matches = withRNG.withColumn("probability", expr("probabilityIn(uuid, 'ids')")).
        filter("probability > 0")
      //matches.show()

      val matchesCount = matches.count()
      countFalsePositive += matchesCount

      val (cidiff, ciunit) = stop(starti)
      println(s"before actual count $i $matchesCount ($countFalsePositive) false positives took $cidiff $ciunit")
      val (collisionsCount, startc) =
        if (matchesCount != 0) {
          // inner join
          val collisions = matches.join(orig, "uuid")
          //collisions.show()

          val startc = System.currentTimeMillis()

          val collisionsCount = collisions.filter("isNotNull(uuid)").count()
          count += collisionsCount
          (collisionsCount, startc)
        } else
          (0, System.currentTimeMillis())

      val (idiff, iunit) = stop(starti)
      val (icdiff, icunit) = stop(startc)
      // count += //matches.filter("probability > 0").count()
      println(s"ran iteration $i added $matchesCount ($countFalsePositive) false positives, $collisionsCount ($count) collisions in $idiff $iunit total, and $icdiff $icunit just for join and count")
    }

    val (diff, unit) = stop(start)

    println(s"took $diff $unit - for ${countFalsePositive} non false rows out of ${numRows * 11} total")
    assert( 0 == count, s"Should have had 0 collisions instead had ${count} per ${numRows * 11} collisions")
  }

}
*/