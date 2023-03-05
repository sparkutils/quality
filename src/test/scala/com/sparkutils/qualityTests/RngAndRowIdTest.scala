package com.sparkutils.qualityTests

import com.sparkutils.quality.impl.bloom.parquet.{BlockSplitBloomFilterImpl, ThreadSafeBloomLookupImpl}
import com.sparkutils.quality.impl.rng.RandomLongs
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BinaryType, LongType, StringType}
import org.junit.Test
import org.scalatest.FunSuite

class RngAndRowIdTest extends FunSuite with RowTools with TestUtils {

  @Test
  def rngBytesTest: Unit = evalCodeGensNoResolve {
    val numRows = 10000
    // obviously can't actually test the values
    val ids = sparkSession.range(numRows)
    val unique = ids.selectExpr("*", "rngBytes() as uuid").
      drop("id").distinct()

    assert(unique.schema.fields.head.dataType == BinaryType)
    assert(unique.count() == numRows)
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
      drop("id").selectExpr("rngUUID(uuid) as uuid").distinct()

    assert(unique.schema.fields.head.dataType == StringType)
    assert(unique.count() == numRows)
  }

  @Test
  def idFromUUIDTest: Unit = evalCodeGensNoResolve {
    val numRows = 10000
    // obviously can't actually test the values
    val ids = sparkSession.range(numRows)
    val unique = ids.selectExpr("*", s"uuid() as uuid").
      drop("id").selectExpr("longPairFromUUID(uuid) as uuid").selectExpr("uuid.lower as lower","uuid.higher as higher").drop("uuid").distinct()

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
      selectExpr("longPair(lower, higher)").distinct()

    assert(unique.schema.fields.head.dataType == RandomLongs.structType)
    assert(unique.count() == numRows)
  }

  @Test
  def saferRowIDNormalRNGTest: Unit = doSaferRowIDTest("rng()", (row: Row) => {
    val r = row.getAs[Row]("uuid")
    Array(r.getAs[Long]("lower"), r.getAs[Long]("higher"))
  }, reallyUnique = true)

  // numRows is 10000 so rand between 0-30000 should "eventually" allow us to finish but force re-evaluation
  @Test
  def saferRowIDBadRNGTest: Unit = doSaferRowIDTest("cast( round( (rand() * 100000) % 10000 ) as bigint)", _.getAs[Long]("uuid"))

  def doSaferRowIDTest(rng: String, rowToA: Row => Any, reallyUnique: Boolean = false): Unit = evalCodeGensNoResolve {
    import com.sparkutils.quality._
    val numRows = 10000
    // obviously can't actually test the values
    val ids = sparkSession.range(numRows)
    val uuids = ids.selectExpr("*", s"$rng as uuid").persist() // have to persist to stop re-evaluation

    val fpp = 0.0001

    val bf = uuids.selectExpr(s"smallBloom(uuid, ${numRows * 5}, cast( $fpp as double) ) as bloom")
    val bits = bf.head().getAs[Array[Byte]](0)
    val filled = bits.count(_ != 0)
    val bloom = ThreadSafeBloomLookupImpl( bits )

    // check the bloom
    import scala.collection.JavaConverters._
    for(row <- uuids.toLocalIterator().asScala) {
      val id = row.getAs[Long]("id")
      val i = rowToA(row)
      assert(bloom.mightContain(i), s"row $id was $i")
    }

    val blooms: BloomFilterMap = Map("ids" -> (bloom, 1 - fpp))
    registerLongPairFunction(blooms)

    val unique = uuids.selectExpr(s"saferLongPair($rng, 'ids') as uuid").distinct()

    /** we should never get collisions */
    assert(unique.join(uuids, "uuid").count() == 0)
    if (reallyUnique) {
      assert(unique.count() == numRows)
    }
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
