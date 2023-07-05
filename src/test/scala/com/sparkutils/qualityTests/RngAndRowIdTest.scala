package com.sparkutils.qualityTests

import com.sparkutils.quality.functions.{long_pair, long_pair_from_uuid, rng_bytes, rng_uuid}
import com.sparkutils.quality.impl.bloom.parquet.{BlockSplitBloomFilterImpl, ThreadSafeBloomLookupImpl}
import com.sparkutils.quality.impl.rng.RandomLongs
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types.{BinaryType, LongType, StringType}
import org.junit.Test
import org.scalatest.FunSuite

class RngAndRowIdTest extends FunSuite with TestUtils {

  @Test
  def rngBytesTest: Unit = evalCodeGensNoResolve {
    val numRows = 10000
    // obviously can't actually test the values
    val ids = sparkSession.range(numRows)
    val unique = ids.selectExpr("*", "rngBytes() as uuid").
      drop("id").distinct()

    assert(unique.schema.fields.head.dataType == BinaryType)
    assert(unique.count() == numRows)

    val check = unique.select(rng_bytes() as "uu")
      .filter("uuid = uu").count()

    assert(check == 0)
  }

  // using partitions to try to force jumps, doesn't seem to actually happen without much larger data though, increasing partitions just takes for ever
  // from the catalyst in spark 3, at least, it may never happen either way - but probably only on a cluster
  @Test
  def rngLongsTest: Unit = evalCodeGensNoResolve {
    val numRows = 10000
    // obviously can't actually test the values
    val ids = sparkSession.range(numRows).repartition(2)
    val unique = ids.selectExpr("*", "rng() as uuid").
      drop("id").selectExpr("uuid.lower as lower","uuid.higher as higher").drop("uuid").distinct()

    assert(unique.schema.fields.head.dataType == LongType)
    assert(unique.schema.fields.last.dataType == LongType)
    assert(unique.count() == numRows)
  }

  @Test
  def rngLongsUUIDTest: Unit = doRngUUIDTest("rng")
  @Test
  def rngBytesUUIDTest: Unit = doRngUUIDTest("rngBytes")

  @Test
  def rngLongsUUIDNonJumpableTest: Unit = doRngUUIDTest("rng", "WELL_44497_B")
  @Test
  def rngBytesUUIDNonJumpableTest: Unit = doRngUUIDTest("rngBytes", "WELL_44497_B")


  def doRngUUIDTest(func: String, rand: String = "XO_RO_SHI_RO_128_PP"): Unit = evalCodeGensNoResolve {
    val numRows = 10000
    // obviously can't actually test the values
    val ids = sparkSession.range(numRows)
    val unique = ids.selectExpr("*", s"$func('$rand') as uuid").
      drop("id").select(expr("rngUUID(uuid) as uuid"), rng_uuid(col("uuid")) as "uuid2").distinct()

    assert(unique.schema.fields.head.dataType == StringType)
    assert(unique.count() == numRows)

    assert(unique.filter("uuid != uuid2").count() == 0)
  }

  @Test
  def idFromUUIDTest: Unit = evalCodeGensNoResolve {
    val numRows = 10000
    // obviously can't actually test the values
    val ids = sparkSession.range(numRows)
    val unique = ids.selectExpr("*", s"uuid() as uuid").
      drop("id").select(expr("longPairFromUUID(uuid) as uuid"), long_pair_from_uuid(col("uuid")) as "uuid2")
      .filter("uuid = uuid2").selectExpr("uuid.lower as lower","uuid.higher as higher").distinct()

    assert(unique.schema.fields.head.dataType == LongType)
    assert(unique.schema.fields.last.dataType == LongType)
    assert(unique.count() == numRows)
  }

  @Test
  def rowIDTest: Unit = evalCodeGensNoResolve {
    val numRows = 10000
    // obviously can't actually test the values
    val ids = sparkSession.range(numRows)
    val unique = ids.selectExpr("*", s"rng() as uuid").
      drop("id").selectExpr("uuid.lower as lower","uuid.higher as higher").drop("uuid").
      select(expr("longPair(lower, higher) pair"), long_pair(col("lower"), col("higher")) as "pair2")
      .filter("pair = pair2").drop("pair2")
      .distinct()

    assert(unique.schema.fields.head.dataType == RandomLongs.structType)
    assert(unique.count() == numRows)
  }

  @Test
  def rngBytesWellsTest: Unit = evalCodeGensNoResolve {
    val numRows = 10000
    // obviously can't actually test the values
    val ids = sparkSession.range(numRows)
    val unique = ids.selectExpr("*", "rngBytes('WELL_44497_B') as uuid").
      drop("id").distinct()

    assert(unique.schema.fields.head.dataType == BinaryType)
    assert(unique.count() == numRows)
  }



}
