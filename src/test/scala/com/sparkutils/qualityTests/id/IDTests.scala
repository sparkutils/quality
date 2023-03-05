package com.sparkutils.qualityTests.id

import com.sparkutils.quality._
import com.sparkutils.quality.impl.hash.{HashFunctionFactory, MessageDigestFactory, ZALongHashFunctionFactory, ZALongTupleHashFunctionFactory}
import com.sparkutils.quality.impl.id._
import com.sparkutils.quality.impl.id.model.{ProvidedID, RandomID}
import com.sparkutils.quality.impl.rng.RandomLongs
import com.sparkutils.quality.utils.BytePackingUtils
import com.sparkutils.qualityTests._
import org.apache.commons.rng.simple.RandomSource
import org.apache.spark.sql.functions._
import org.apache.spark.sql.qualityFunctions.DigestFactory
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.junit.Test
import org.scalameter.api.{Bench, Gen}
import org.scalatest.FunSuite

import java.security.{MessageDigest, Provider}
import java.util.Base64
import scala.jdk.CollectionConverters._

class IDTests extends FunSuite with RowTools with TestUtils {

  @Test
  def rountTripRandom: Unit = doRoundTripGenericLongBasedID(model.RandomID)
  @Test
  def rountTripProvided: Unit = doRoundTripGenericLongBasedID(model.ProvidedID)
  @Test
  def rountTripFields: Unit = doRoundTripGenericLongBasedID(model.FieldBasedID)

  def doRoundTripGenericLongBasedID(idType: IDType): Unit = evalCodeGensNoResolve {

    val longs = Array(1235564L, 1455535634L, 1235453L, 1L, 100L)

    val randID = GenericLongBasedID(idType, longs)

    val base64ID = randID.base64

    val base = randID.base

    val ar = Array.ofDim[Byte](4)
    BytePackingUtils.encodeInt(base, 0, ar)
    assert(model.GenericLongsHeader == ar(0))

    val bytes = Base64.getDecoder.decode(base64ID)
    // take the first 4
    val idtype = bytes(0)
    assert(model.GenericLongsHeader == idtype)

    val serder = model.parseID(base64ID).asInstanceOf[GenericLongBasedID]
    assert(serder.array.length == longs.length)
    assert(serder.base64 == base64ID)
  }

  @Test
  def assertsOnGuaranteedUniqueID: Unit = evalCodeGensNoResolve {

    var passed = false
    try {
      val badhardwareAddress = Array.ofDim[Byte](3210)
      GuaranteedUniqueID(badhardwareAddress, System.currentTimeMillis(), 1234, 125545154)
      passed = true
    } catch {
      case e: AssertionError =>
        assert(e.getMessage.contains("MAC addresses are only ever 48bit"))
    }
    assert(passed == false, "Should have thrown given larger than 6 array size")

    passed = false
    try {
      val hardwareAddress = Array.ofDim[Byte](6)
      GuaranteedUniqueID(hardwareAddress, System.currentTimeMillis(), 1234, model.guaranteedUniqueMaxRowID)
      passed = true
    } catch {
      case e: AssertionError =>
        assert(e.getMessage.contains("RowID cannot be more than 31bits long i.e. "))
    }
    assert(passed == false, "Should have thrown given larger than guaranteedUniqueMaxRowID rowid")


    passed = false
    try {
      val hardwareAddress = Array.ofDim[Byte](6)
      // causes overflow on 31 bits
      GuaranteedUniqueID(hardwareAddress, System.currentTimeMillis(), 1234, model.guaranteedUniqueMaxRowID + 1000000)
      passed = true
    } catch {
      case e: AssertionError =>
        assert(e.getMessage.contains("RowID must be greater than 0 not "))
    }
    assert(passed == false, "Should have thrown given overflowed rowid")

    passed = false
    try {
      val hardwareAddress = Array.ofDim[Byte](6)
      // already 41bits on currentTimeMillis, bump it up a digit to break through some fabric
      GuaranteedUniqueID(hardwareAddress, System.currentTimeMillis() * 10, 1234, model.guaranteedUniqueMaxRowID - 1)
      passed = true
    } catch {
      case e: AssertionError =>
        assert(e.getMessage.contains("Tardis"))
    }
    assert(passed == false, "Should have thrown given larger than 41bit timestamp")
  }

  @Test
  def roundTripGuaranteedUniqueIDLocalMac: Unit = evalCodeGensNoResolve {
    doRoundTripGuaranteedUniqueID(model.localMAC)
  }

  def doRoundTripGuaranteedUniqueID(hardwareAddress: Array[Byte]): Unit = {
    val ms = 42815340251L // force an odd number ms so all the bits are needed in array(2) - System.currentTimeMillis() - model.guaranteedUniqueEpoch
    val uniqueID = GuaranteedUniqueID(hardwareAddress, ms, 1234, model.guaranteedUniqueMaxRowID - 1 )
    val uniqueID2 = GuaranteedUniqueID(hardwareAddress, ms, 1234, model.guaranteedUniqueMaxRowID - 1 )

    val base64ID = uniqueID.base64
    assert(uniqueID2.base64 == base64ID, "oddly generated different base64 for same inputs")

    val serder = model.parseID(base64ID).asInstanceOf[GuaranteedUniqueID]

    assert(serder.base == uniqueID.base, "should have same base")
    assert(serder.array(0) == uniqueID.array(0), "should have same 1st long")
    assert(serder.ms == ms, "If everything else is matching then so should the timestamp from the input")
    assert(serder.row == model.guaranteedUniqueMaxRowID - 1, "If everything else is matching then so should the row from the input")
    assert(serder.array(1) == uniqueID.array(1), "should have same 2nd long")
    assert(serder.base64 == base64ID, "should have same base64")
  }

  /***
   * 66-16-a-ffffffa5-fffffff8-fffffff3 being created on gitlab but not serialized properly
   */
  @Test
  def guaranteedUniqueIDMACAddressOverflowTest: Unit = evalCodeGensNoResolve {
    val hardwareAddress = Array[Byte](0xffffffff, 0x0, 0xa, 0xffffffa5, 0xfffffff8, 0xfffffff3)
    doRoundTripGuaranteedUniqueID(hardwareAddress)
  }

  @Test
  def testGuaranteedUniqueIDOps: Unit = evalCodeGensNoResolve {
    import java.net._
    import scala.collection.JavaConverters._

    val nonNulls = NetworkInterface.getNetworkInterfaces.asScala map (_.getHardwareAddress) filter (_ != null)
    val hardwareAddress: Array[Byte] = nonNulls.next

    assert(model.localMAC.zip(hardwareAddress).forall(p => p._1 == p._2), "Should have identical local mac")

    val uniqueID = GuaranteedUniqueID(mac = hardwareAddress, partition = 1234, row = model.guaranteedUniqueMaxRowID - 2)
    assert(model.localMAC.zip(uniqueID.mac).forall(p => p._1 == p._2), "Should have identical default to local mac")

    val ops = uniqueID.uniqueOps
    assert(ops.base64 == uniqueID.base64, "should have same base64 as we've not done anything yet")

    // adding a row should produce the same
    val rowByOne = uniqueID.copy(row = uniqueID.row + 1)
    ops.incRow
    assert(ops.base64 == rowByOne.base64, "rowByOne - should have same base64")
    assert(ops.ms == uniqueID.ms, "Should have had the same ms as the original")

    // this time round the inc *should* push the ms up
    Thread.sleep(10)
    ops.incRow
    ops.incRow
    val serder = model.parseID(ops.base64).asInstanceOf[GuaranteedUniqueID]

    assert(serder.ms >= (uniqueID.ms + 10), "should have incremented the time")
    assert(serder.row == 0, "should have reset the row to zero")
    assert(serder.base == uniqueID.base, "should have the same base")
    assert(serder.partition == uniqueID.partition, "should have the same partition")
    assert(model.localMAC.zip(serder.mac).forall(p => p._1 == p._2), "Should have identical default to local mac")
    assert(ops.ms == serder.ms, "Serialised after ms re-evaluation should be same as original")
  }

  @Test
  def testRNGIDGen: Unit = evalCodeGensNoResolve {
    import com.sparkutils.quality._
    registerQualityFunctions()

    def testRes(rngExploded: DataFrame): Unit = {
      rngExploded.show
      assert(rngExploded.schema.fields.map(_.name).toSeq
        == Seq("id", "rng_id_base", "rng_id_i0", "rng_id_i1"), "Column names incorrect")
    }


    val df = sparkSession.range(0, 6000)
    val rngExploded = df.withColumn("rng_id", rngID("rng_id")).selectExpr("id","rng_id.*")
    testRes(rngExploded)

    val rngExplodedSQL = df.selectExpr("*", "rngid('rng_id') as rng_id").selectExpr("id","rng_id.*")
    testRes(rngExplodedSQL)
  }

  @Test
  def testRNGIDGenNonJump: Unit = evalCodeGensNoResolve {
    import com.sparkutils.quality._
    registerQualityFunctions()

    def testRes(rngExploded: DataFrame): Unit = {
      rngExploded.show
      assert(rngExploded.schema.fields.map(_.name).toSeq
        == Seq("id", "rng_id_base", "rng_id_i0", "rng_id_i1"), "Column names incorrect")
    }

    def nonJump(prefix: String) =
      new Column(GenericLongBasedIDExpression(model.RandomID,
        RandomLongs(RandomSource.KISS).expr, prefix))

    val df = sparkSession.range(0, 6000)
    val rngExploded = df.withColumn("rng_id", nonJump("rng_id")).selectExpr("id","rng_id.*")
    testRes(rngExploded)
  }

  @Test
  def testSHA256IDGen: Unit = evalCodeGensNoResolve  {
    doFieldGenTest("SHA-256", "digestToLongsStruct", longCount = 4)
  }

  @Test
  def testMD5IDGen: Unit = evalCodeGensNoResolve  {
    doFieldGenTest("MD5", "digestToLongsStruct")
  }

  @Test
  def testSHA256IDGenHashFun: Unit = evalCodeGensNoResolve  {
    doFieldGenTest("SHA-256", "hashWithStruct", longCount = 4)
  }

  @Test
  def testMD5IDGenHashFun: Unit = evalCodeGensNoResolve  {
    doFieldGenTest("MD5", "hashWithStruct")
  }

  @Test
  def testMURMUR3_128IDGenHashFun: Unit = evalCodeGensNoResolve  {
    doFieldGenTest("MURMUR3_128", "hashWithStruct", "hashFieldBasedID", HashFunctionFactory(_))
  }

  @Test
  def testXXH3IDGenZAHashFun: Unit = evalCodeGensNoResolve  {
    doFieldGenTest("XXH3", "zaHashLongsWithStruct", "zaLongsFieldBasedID", ZALongTupleHashFunctionFactory)
  }

  @Test
  def testMURMUR3_128IDZAGenHashFun: Unit = evalCodeGensNoResolve  {
    doFieldGenTest("MURMUR3_128", "zaHashLongsWithStruct", "zaLongsFieldBasedID", ZALongTupleHashFunctionFactory)
  }

  @Test
  def testMURMUR3IDZAGenHashFun: Unit = evalCodeGensNoResolve  {
    doFieldGenTest("MURMUR3", "zaHashWithStruct", "zaFieldBasedID", ZALongHashFunctionFactory, 1)
  }

  /**
   * should generate a 32bit which is padded to 64, fake digest to trigger this
   */
  @Test
  def testFakeIDGenDigestFun: Unit = not_Databricks {
    class TwoByteProvider extends Provider("TwoByte", 0.1, "fake digest") {
      put("MessageDigest.TwoByte", classOf[TwoByteDigest].getName)
    }
    java.security.Security.addProvider(new TwoByteProvider)
    evalCodeGensNoResolve  {
      doFieldGenTest("TwoByte", "digestToLongsStruct", digestFactory = MessageDigestFactory, longCount = 1)
    }
  }

  /**
   * should generate a 32bit which is padded to 64
   */
  @Test
  def testAdlerIDGenHashFun: Unit = evalCodeGensNoResolve  {
    doFieldGenTest("ADLER32", "hashWithStruct", "hashFieldBasedID", HashFunctionFactory(_), 1)
  }

  def doFieldGenTest(digestImpl: String, digestFun: String, fieldBasedId: String = "fieldBasedID", digestFactory: String => DigestFactory = MessageDigestFactory, longCount: Int = 2 ): Unit = {
    import com.sparkutils.quality._
    registerQualityFunctions()
    import sparkSession.implicits._

    def testRes(md5Exploded: DataFrame): Unit = {
      md5Exploded.show
      val slice = Seq("id", "md5_id_base") ++ (0 until longCount).map(i => s"md5_id_i$i")
      assert(md5Exploded.schema.fields.map(_.name).toSeq
        .containsSlice(slice), "Column names incorrect")
      assert(md5Exploded.schema.fields.length == 2 + longCount )
    }

    val df = sparkSession.range(0, 6000).selectExpr("id", "id || '_field' as f1", "id || '_field2' as f2", "id || '_field3' as f3")
    val md5Exploded = df.withColumn("md5_id", fieldBasedID("md5_id", Seq($"f1", $"f2", $"f3"), digestImpl, digestFactory)).selectExpr("id","md5_id.*")
    testRes(md5Exploded)

    // same with text version
    val md5Res = df.selectExpr("*", s"$digestFun('$digestImpl', f1, f2, f3) as digest" ).withColumn("md5_id", providedID("md5_id", $"digest")).selectExpr("id","md5_id.*")
    testRes(md5Res)

    assert(md5Exploded.union(md5Res).distinct.count == md5Exploded.count, "should be able to diff these to the same count...")

    val sqlDirect = df.selectExpr("*", s"$fieldBasedId('md5_id', '$digestImpl', f1, f2, f3) as digest" ).selectExpr("id","digest.*")
    testRes(sqlDirect)

    assert(md5Exploded.union(sqlDirect).distinct.count == md5Exploded.count, "should be able to diff these to the same count sqldirect...")

    val sqlExp = df.selectExpr("*", s"providedID('md5_id', $digestFun('$digestImpl', f1, f2, f3)) as md5_id").selectExpr("id","md5_id.*")
    testRes(sqlExp)

    assert(md5Exploded.union(sqlExp).distinct.count == md5Exploded.count, "should be able to diff these to the same count sqlExp...")

  }

  @Test
  def testMurmur3: Unit = evalCodeGensNoResolve {
    import com.sparkutils.quality._
    registerQualityFunctions()
    import sparkSession.implicits._

    def testRes(md5Exploded: DataFrame): Unit = {
      md5Exploded.show
      assert(md5Exploded.schema.fields.map(_.name).toSeq
        .containsSlice( Seq("id", "md5_id_base", "md5_id_i0", "md5_id_i1")), "Column names incorrect")
    }

    val df = sparkSession.range(0, 6000).selectExpr("id", "id || '_field' as f1", "id || '_field2' as f2", "id || '_field3' as f3")
    val md5Exploded = df.withColumn("md5_id", murmur3ID("md5_id", Seq($"f1", $"f2", $"f3"))).selectExpr("id","md5_id.*")
    testRes(md5Exploded)

    val md5ExplodedStar = df.withColumn("md5_id", murmur3ID("md5_id", $"f1", $"f2", $"f3")).selectExpr("id","md5_id.*")
    testRes(md5ExplodedStar)

    // same with text version
    val md5Res = df.selectExpr("*", s"murmur3ID('md5_id', f1, f2, f3) as md5_id" ).selectExpr("id","md5_id.*")
    testRes(md5Res)
  }

  @Test
  def testUniqueIDGen: Unit = evalCodeGensNoResolve {
    import com.sparkutils.quality._
    registerQualityFunctions()

    val df = sparkSession.range(0, 6000)
    val uniqueExploded = df.withColumn("unique_id", uniqueID("unique_id")).selectExpr("id","unique_id.*")
    uniqueExploded.show
    assert(uniqueExploded.schema.fields.map(_.name).toSeq
      == Seq("id", "unique_id_base", "unique_id_i0", "unique_id_i1"), "Column names incorrect")

    def getBase64(row: Row) =
      model.base64(160, row.getAs[Int]("unique_id_base"),
        Array(row.getAs[Long]("unique_id_i0"), row.getAs[Long]("unique_id_i1")))

    val rowSample164 = getBase64(uniqueExploded.head)
    val gen = model.parseID(rowSample164).asInstanceOf[GuaranteedUniqueID]

    Thread.sleep(1)

    val rowSample264 = getBase64(uniqueExploded.head)
    val gen2 = model.parseID(rowSample264).asInstanceOf[GuaranteedUniqueID]

    assert(gen.ms != gen2.ms, "Separate actions need separate ms")

    val uniqueExplodedSQL = df.selectExpr("*", "uniqueid('unique_id') as unique_id").selectExpr("id","unique_id.*")
    uniqueExplodedSQL.show
    assert(uniqueExplodedSQL.schema.fields.map(_.name).toSeq
      == Seq("id", "unique_id_base", "unique_id_i0", "unique_id_i1"), "Column names incorrect")

    val sqlHead = getBase64(uniqueExplodedSQL.head)
    val sqlID = model.parseID(sqlHead).asInstanceOf[GuaranteedUniqueID]
    assert(sqlID.mac.zip( gen.mac ).forall(p => p._1 == p._2), "Should have had the same mac if the right algo was used")
    assert(sqlID.base == gen.base, "Should have had the same base if the right algo was used")
  }

  @Test
  def testIDEqual: Unit = evalCodeGensNoResolve {
    import com.sparkutils.quality._
    registerQualityFunctions()

    val df = sparkSession.range(0, 6000)
    val uniqueExploded = df.withColumn("unique_id", uniqueID("unique_id")).selectExpr("id","unique_id.*")
    val cached = uniqueExploded.cache

    val count = uniqueExploded.count

    val renamed = cached.selectExpr("unique_id_base as unid_base", "unique_id_i0 as unid_i0", "unique_id_i1 as unid_i1")
    val after = renamed.join(cached, expr("idEqual('unique_id', 'unid')")).count
    assert(after == count, "idEqual should have joined them fully")
  }

  @Test
  def testUUIDRoundTripping: Unit = evalCodeGensNoResolve {
    import com.sparkutils.quality._
    registerQualityFunctions()

    val df = sparkSession.range(0, 6000)
    val uuidExploded = df.selectExpr("uuid() as uuid").selectExpr("*", "providedId('pre', longPairFromUUID(uuid)) as pid").
      selectExpr("pid","uuid","rngUUID(prefixedToLongPair('pre', pid)) as rere")

    uuidExploded.toLocalIterator.asScala.foreach {
      row =>
        assert(row.getString(1) == row.getString(2))//uuid should be rere
    }
  }

  @Test
  def equalsTest: Unit = {
    val a1 = Array.ofDim[Long](1)
    val a2 = Array(0L, 1L)
    val a3 = Array(1L, 1L)
    val a4 = Array(1L, 1L)
    /*  val RandomID: IDType = IDTypeImpl(Integer.parseInt("0000", 2).toByte, isRandom = true)
      val FieldBasedID: IDType = IDTypeImpl(Integer.parseInt("0010", 2).toByte, isFieldBased = true)
      val GuaranteedUniqueIDType: IDType = IDTypeImpl(Integer.parseInt("0001", 2).toByte, isGuaranteedUnique = true)
      val ProvidedID: IDType = IDTypeImpl(Integer.parseInt("0100", 2).toByte, isProvided = true)
  */
    val oneProvided = GenericLongBasedID(ProvidedID, a1)
    val twoProvided = GenericLongBasedID(ProvidedID, a2)

    assert(!oneProvided.comparableImplementation(twoProvided))
    assert(oneProvided != twoProvided)

    val threeProvided = GenericLongBasedID(ProvidedID, a3)
    assert(threeProvided != twoProvided)

    val fourProvided =  GenericLongBasedID(ProvidedID, a4)
    assert(threeProvided == fourProvided)

    val rand = GenericLongBasedID(RandomID, a4)
    assert(!rand.comparableImplementation(threeProvided))
    assert(threeProvided != rand)
  }
}

import org.scalameter.api._

object SumIdGenTest extends Bench.OfflineReport with RowTools {
  import sparkSession.implicits._

  import scala.collection.JavaConverters._

  val rowgenerator = Gen.range("rows")(20000, 3000000, 50000)

  performance of "cachedIDGen" config (
    exec.minWarmupRuns -> 2,
    exec.maxWarmupRuns -> 4,
    exec.benchRuns -> 4,
    exec.jvmcmd -> (System.getProperty("java.home")+"/bin/java"),
    exec.jvmflags -> List("-Xmx8g","-Xms8g")
    //  verbose -> true
  ) in {
    measure method "rngBased" in {
      using(rowgenerator) in evaluate(_.withColumn("rng_id", rngID("rng_id")).selectExpr("id","rng_id.*"), "rng_id_i0")
    }
    measure method "globalIDBased" in {
      using(rowgenerator) in evaluate(_.withColumn("unique_id", uniqueID("unique_id")).selectExpr("id","unique_id.*"), "unique_id_i0")
    }
    measure method "md5Based" in {
      using(rowgenerator) in evaluate(_.withColumn("murmur3ID", murmur3ID("unique_id", $"f1", $"f2", $"f3")).selectExpr("id","murmur3ID.*"), "unique_id_i0")
    }
    measure method "fieldBasedID" in {
      using(rowgenerator) in evaluate(_.withColumn("fieldBasedID", fieldBasedID("unique_id", "MD5", $"f1", $"f2", $"f3")).selectExpr("id","fieldBasedID.*"), "unique_id_i0")
    }
  }

  def evaluate(func: (DataFrame) => DataFrame, colname: String)(param: Int) = {
    // the extra fields are added so performance of the other alternatives can be managed
    val df = sparkSession.range(0, param).selectExpr("id", "id || '_field' as f1", "id || '_field2' as f2", "id || '_field3' as f3")

    val ndf = func(df)

    val sum =
      ndf.toLocalIterator().asScala.map { _.getAs[Long](colname) }.sum // get will probably be dumped, but hopefully not
    sum
  }

}

class TwoByteDigest extends MessageDigest("TwoByte") with Cloneable {
  def engineUpdate(input: Byte): Unit = {}

  def engineUpdate(input: Array[Byte], offset: Int, len: Int): Unit = {}

  def engineDigest(): Array[Byte] = Array(1.toByte, 2.toByte)

  def engineReset(): Unit = {}

  override def clone(): AnyRef = new TwoByteDigest()
}
