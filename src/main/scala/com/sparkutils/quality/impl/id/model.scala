package com.sparkutils.quality.impl.id

import java.util.{Base64, Calendar, TimeZone}

import com.sparkutils.quality.utils.BytePackingUtils
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}

import scala.collection.immutable.BitSet

sealed trait IDType extends Serializable {
  /**
   * Is this ID statistically likely to be unique - but not guaranteed?
   */
  val isRandom: Boolean
  /**
   * Is this ID based on other fields in the existing dataset, e.g. a Data Vault md5?
   */
  val isFieldBased: Boolean
  /**
   * Is this field guaranteed to be unique?
   */
  val isGuaranteedUnique: Boolean
  /**
   * Is this a provided ID, e.g. not maintained by any Quality functionality
   */
  val isProvided: Boolean
  /**
   * A mask to allow identification of type
   */
  val typeMask: Byte
}

private case class IDTypeImpl(typeMask: Byte,
  isRandom: Boolean = false, isFieldBased: Boolean = false,
  isGuaranteedUnique: Boolean = false, isProvided: Boolean = false) extends IDType with Serializable

/**
 * Represents an extensible ID starting with 160bits.
 *
 * There are four known usages:
 * - Rng (128bit PRNG using the rng function)
 * - Data Vault (128bit MD5 based on user provided columns)
 * - Snowflake (160bit globally unique identifier based on Twitters approach to unique IDs with a Spark twist)
 * - Provided (upstream provided identifier - variable bit length + 4 for type )
 *
 * RNG / Provided and Field based are all the same implementation.
 *
 * Implementations must use the first 4 bits to signify which of the base four types are used with plus extension.
 */
sealed trait ID {
  /**
   * Is this ID based on a series of opaque Long encodings, e.g. provided, data vault style hashing or prng based
   */
  val isGenericLongBased: Boolean

  /**
   * Type of this ID
   */
  val idType: IDType

  /**
   * Identifies the first 4 bits
   */
  val header: Byte

  /**
   * BitSet representation of the ID
   * @return
   */
  def bitset: BitSet

  /**
   * Bit length of this implementation
   * @return
   */
  def bitLength: Int

  /**
   * base64 representation - matches the factory function in ID
   * @return
   */
  def base64: String

  /**
   * Spark representation, typically these are also exploded, use dataType function to get exploding prefixes
   * @return
   */
  def rawDataType: StructType

  /**
   * Data type to explode fields to, should be used as the dataType in Expressions
   * @param prefix prefix_ is used to prefix every direct field of this ID
   * @return
   */
  def dataType(prefix: String): StructType =
    StructType( rawDataType.fields.map(f => f.copy( name = prefix +"_"+f.name ) ) )

  /**
   * Whilst IDs can be naively compared this returns if they are compatible implementations.  For example comparing a
   * 128bit random number to 256 would be incomparable despite both being random
   * @param other
   * @return
   */
  def comparableImplementation(other: ID): Boolean
}

/**
 * Represents an ID using base with length and 4 bit leading type and an array of longs
 */
sealed trait BaseWithLongs extends ID {
  /**
   * information about length and type
   */
  def base: Int =
    (array.length << model.lengthOffset) |
      (header << model.headerBaseBitsOffset )

  /**
   * the payload itself - maximum 65k which will be far above a max Row size
   */
  def array: Array[Long]

  /**
   * Spark representation, typically these are also exploded, use dataType function to get exploding prefixes
   * @return
   */
  override def rawDataType: StructType = StructType(
    ( StructField(name = "base", dataType = IntegerType) +:
      (0 until array.length).map(i => StructField(name = "i"+i, dataType = LongType)) ).toArray
  )

  /**
   * BitSet representation of the ID
   *
   * @return
   */
  override def bitset: BitSet = {
    val ar = Array.ofDim[Long](array.length + 1)
    ar(0) = base
    Array.copy(array, 0, ar, 1, array.length)
    var bitset = BitSet.fromBitMaskNoCopy( ar ) // we don't modify it so no need to copy
    bitset
  }

  /**
   * Bit length of this implementation
   *
   * @return
   */
  override def bitLength: Int = 32 + (array.length * 64)

  /**
   * base64 representation - matches the factory function in ID
   *
   * @return
   */
  override def base64: String = model.base64(bitLength, base, array)

}

/**
 * Represents a GenericLongBasedID
 */
case class GenericLongBasedID(idType: IDType, array: Array[Long]) extends BaseWithLongs {
  override val isGenericLongBased: Boolean = true

  /**
   * Whilst IDs can be naively compared this returns if they are compatible implementations.  For example comparing a
   * 128bit random number to
   *
   * @param other
   * @return
   */
  override def comparableImplementation(other: ID): Boolean =
    other match {
      case id: GenericLongBasedID if idType == id.idType && id.bitLength == this.bitLength => true
      case _ => false
    }

  /**
   * The header information itself may not be the same
   * @param obj
   * @return
   */
  override def equals(obj: Any): Boolean =
    obj match {
      case id: GenericLongBasedID if comparableImplementation(id) =>
        array.zip(id.array).forall(p => p._1 == p._2)
      case _ => false
    }

  /**
   * Identifies the first 4 bits
   */
  override val header: Byte = model.GenericLongsHeader
}

/**
 * Manipulations on the long values directly for use in expressions etc.
 * Use base64 to marshall as needed
 */
class GuaranteedUniqueIDOps(val base: Int){
  // package private later?
  val array = Array.ofDim[Long](2)
  var row = 0

  // can just be removed, but doesn't add much overhead whilst providing decent extra checks
  var ms = 0L

  /**
   * Increments the row count
   */
  def incRow = {
    if (row < model.guaranteedUniqueMaxRowID) {
      row += 1
    } else {
      row = 0
      // only do this when needed, it's expensive
      ms = (System.currentTimeMillis - model.guaranteedUniqueEpoch) & model.guaranteedUniqueMSMask

      // last bit of array(0) if first 8 bits of ms
      array(0) = array(0) & model.allButLast8 | (ms >> model.guaranteedUniqueMSLastIndex) & model.all64
      // first 5, wipe out the counter first
      array(1) = 0
      array(1) = array(1) & model.allButFirst33 |
        (ms << model.guaranteedUniqueMSLastShifted) & model.all64
    }
    array(1) = array(1) & model.allButLast31 | row & model.all64
  }

  def base64: String = model.base64(160, base, array)
}

/**
 * Represents a Guaranteed globally unique 160 bit ID within spark based on twitters 64bit snowflake id:
 * leading 4 bits - type of id
 * padded 4 bits for future use / ease of code
 * next 48 bits - MAC address of the drivers host network card (unique for run with no central server / service requirement)
 * next 32 bits - partition id
 * next 41 bits - ms since 20210101
 * padded 7 bits - for future use / ease of code
 * remaining 24 bits - partition specific incremented row id - allowing 16777216 rows per ms, which is unlikely to occur
 * and easy to manage overflow either way
 *
 * The organisation allows the base and first long to be fixed at the start of a partition reset with only the last long
 * changing per row easing bit fiddling.  In the case of overflow the header remains untouched and only then must 41 bits ms value be
 * re-evaluated, reducing clock slow downs (coincidentally around 41ms).
 *
 * Full time synchronisation across all clusters in a business is not required as MAC address provides driver uniqueness
 * (assuming your cloud / network provider guarantees this uniqueness for routing).  Partition id provides segregation across
 * a given action with timestamp ensuring repeated runs on the same cluster do not overlap.
 */
case class GuaranteedUniqueID(mac: Array[Byte] = model.localMAC, ms: Long = (System.currentTimeMillis() - model.guaranteedUniqueEpoch), partition: Int = 0, row: Int = 0) extends BaseWithLongs with Serializable {
  assert(mac.length == 6, "MAC addresses are only ever 48bit")
  assert(row >= 0, s"RowID must be greater than 0 not ${row}")
  assert(row < model.guaranteedUniqueMaxRowID, s"RowID cannot be more than 31bits long i.e. ${model.guaranteedUniqueMaxRowID} instead of $row")
  assert((ms & model.guaranteedUniqueMSMask) == ms, s"ms may not be larger than 48bits is $ms, ensure either " +
    "model.guaranteedUniqueEpoch is subtracted or you used a Tardis.  Tardis usage by humans is strictly prohibited.")

  override val idType: IDType = model.GuaranteedUniqueIDType
  override val isGenericLongBased: Boolean = false

  /**
   * After serializing to a remote instance for mac / any initial values
   * @return
   */
  def uniqueOps = {
    val res = new GuaranteedUniqueIDOps(base)
    val ar = array
    res.array(0) = ar(0)
    res.array(1) = ar(1)
    res.row = row
    res.ms = ms
    res
  }

  /**
   * contains the 4 bits type offset and 24 bits of the mac
   */
  override def base: Int =
    (header << model.headerBaseBitsOffset) | // nothing more to do for header here
      ((mac(0) & 0xFF) << model.guaranteedUniqueMACHeader1stOffset) |
      ((mac(1) & 0xFF) << model.guaranteedUniqueMACHeader2ndOffset) |
      ((mac(2) & 0xFF) << model.guaranteedUniqueMACHeader3rdOffset)

  /**
   * Packs the remaining 3 bytes of mac, partition and the first 8bits of timestamp (48bits as well for ease of packing)
   * into the first long, then the remaining 40bits of the ms and 24bits of row id are packed into the second long.
   */
  override val array: Array[Long] = {
    val trimmedMS = ms & model.guaranteedUniqueMSMask // drop anything else not in the 41bits, shouldn't happen but still

    // should round trip
    var lbits = Array.ofDim[Byte](8)
    lbits(0) = mac(3)
    lbits(1) = mac(4)
    lbits(2) = mac(5)

    BytePackingUtils.encodeInt(partition, 3, lbits)

    // start at 2 for 48 bits
    lbits(7) = 0// lbits2(2)

    var first = BytePackingUtils.unencodeLong(0, lbits)
    var second = 0L

    // last bit of array(0) if first 8 bits of ms - will likely be 0 for a decade or so
    first = first & model.allButLast8 | (trimmedMS >> model.guaranteedUniqueMSLastIndex) & model.all64
    // first 5 bytes for the remaining ms - will likely be exactly ms for a decade or so
    second = second & model.allButFirst33 | // move << 24 makes the 40 at top, leaving 24 over
      (trimmedMS << model.guaranteedUniqueMSLastShifted) & model.all64
    second = second & model.allButLast31 | row & model.all64

    Array(first, second)
  }

  /**
   * Whilst IDs can be naively compared this returns if they are compatible implementations.  For example comparing a
   * 128bit random number to
   *
   * @param other
   * @return
   */
  override def comparableImplementation(other: ID): Boolean =
    other match {
      case id: GuaranteedUniqueID if id == other => true
      case _ => false
    }

  /**
   * Identifies the first 4 bits
   */
  override val header: Byte = model.GuaranteedUniqueHeader
}


case class InvalidIDType(idType: Byte) extends RuntimeException(s"Invalid ID Type $idType")

/**
 * Model for ID handling
 */
object model {
  /**
   * Provides the host's MAC address
   * @return
   */
  val localMAC = {
    import java.net._
    import scala.collection.JavaConverters._

    val nonNulls = NetworkInterface.getNetworkInterfaces.asScala map (_.getHardwareAddress) filter (_ != null)

    // the below doesn't work on some unixes (DBR included), gitlab runners are fine however, ni is null
    /*val localHost = InetAddress.getLocalHost()
    val ni = NetworkInterface.getByInetAddress(localHost)
    val hardwareAddress: Array[Byte] = ni.getHardwareAddress()
    */
    val hardwareAddress: Array[Byte] = nonNulls.next
    //dumpMap(hardwareAddress, "model.localMAC")
    hardwareAddress
  }

  private[id] def dumpMap(hardwareAddress: Array[Byte], info: String) =
    println(s"Quality $info MAC Address read as ${hardwareAddress.map(_.toHexString).mkString("-")}")

  // positions

  /**
   * Length offset for BaseArray based IDs Base Int (last 16 bits)
   */
  val lengthOffset = 0
  /**
   * header bits offset for base Int (first 4 bits)
   */
  val headerBaseBitsOffset = 24 // no longer packing !!! arghh !!!! 8

  // types - can expand / bit map later as needed, 128 values to play with
  val GenericLongsHeader: Byte = 0x00
  val GuaranteedUniqueHeader: Byte = 0x01

  val byteSize = 8

  // MAC address for guaranteed unique

  val guaranteedUniqueMACHeader1stOffset: Int = headerBaseBitsOffset - byteSize
  val guaranteedUniqueMACHeader2ndOffset: Int  = guaranteedUniqueMACHeader1stOffset - byteSize
  val guaranteedUniqueMACHeader3rdOffset: Int  = guaranteedUniqueMACHeader2ndOffset - byteSize

  // keep alignment in code please, it's a pain to figure out otherwise
  // MS Mask used to wipe out highest significance at 41bits, the constructor checks it doesn't overflow
  val guaranteedUniqueMSMask =          0x00000DFFFFFFFFFFL

  // last31 used for the rowid in the second long
  val allButLast31 =                    0xFFFFFFFFE0000000L
  // last 8 used by the start of the ms value in the first long
  val allButLast8 =                     0xFFFFFFFFFFFFFF00L
  // first 5 used for 2nd part of the MS into the second long
  val allButFirst33 =                   0x00000000FFFFFFFFL

  val all64 =                           0xFFFFFFFFFFFFFFFFL

  // used to bit shift to get last 40
  val guaranteedUniqueMSLastIndex = (41 - 8)
  val guaranteedUniqueMSLastShifted: Int = 64 - guaranteedUniqueMSLastIndex // push it to the front of the long

  /**
   * Unique row id's maximum value per ms / partition / mac combination, when a partition uses the max value it will
   * then re-evaluate the ms after epoch.
   * NB 16777216 for 24 bit, 1073741824 for 31
   */
  val guaranteedUniqueMaxRowID = Math.pow(2, 64 - guaranteedUniqueMSLastShifted - 1).toInt

  /**
   * Epoch is chosen as 2020.1.1:0.0.0 which gives 16 years of ms resolution under 40bits, but 33 under 41bits - 41 therefore attempted
   */
  val guaranteedUniqueEpoch = {
    val c = Calendar.getInstance()
    c.setTimeZone(TimeZone.getTimeZone("UTC"))
    c.set(2020, 1, 1, 0, 0, 0)
    c.getTimeInMillis
  }

  val RandomID: IDType = IDTypeImpl(Integer.parseInt("0000", 2).toByte, isRandom = true)
  val FieldBasedID: IDType = IDTypeImpl(Integer.parseInt("0010", 2).toByte, isFieldBased = true)
  val GuaranteedUniqueIDType: IDType = IDTypeImpl(GuaranteedUniqueHeader, isGuaranteedUnique = true)
  val ProvidedID: IDType = IDTypeImpl(Integer.parseInt("0100", 2).toByte, isProvided = true)
/*
  /**
   * Creates the appropriate ID type using the header in base
   * @param base
   * @param longs
   * @return
   */
  def id(base: Int, longs: Array[Long]): ID = {
    val header = (base >> 24).asInstanceOf[Byte]
    val idtype = header & GuaranteedUniqueHeader
    idtype match {
      case GenericLongsHeader => GenericLongBasedID(idTypeOf(header), longs)
      case GuaranteedUniqueHeader => GuaranteedUniqueID()
    }
  } */

  /**
   * For a given byte array provides the length, only relevant for GenericLongsBasedID
   * @param bytes
   * @return
   */
  def lengthOfID(bytes: Array[Byte]) =
    ((bytes(2) & 0xFF) << 16) |
      ((bytes(3) & 0xFF) << 0)

  /**
   * Provides the type of the id from the first byte
   * @param header
   * @return
   */
  def idTypeOf(header: Byte): IDType = {
    val idtype = header & GuaranteedUniqueHeader
    idtype match {
      case GenericLongsHeader =>
        header match {
          case FieldBasedID.typeMask => FieldBasedID
          case ProvidedID.typeMask => ProvidedID
          case RandomID.typeMask => RandomID
          case _ => throw new InvalidIDType(header.toByte)
        }
      case GuaranteedUniqueHeader =>
        GuaranteedUniqueIDType
    }
  }

  /**
   * parses a given base64 representation of an ID into the correct type
   */
  def parseID(base64ID: String): ID = {
    val bytes = Base64.getDecoder.decode(base64ID)
    // take the first 4
    val header = bytes(0)
    val idtype = header & GuaranteedUniqueHeader
    idtype match {
      case GenericLongsHeader => // use 2 and 3 to get the length
        val length = lengthOfID(bytes)
        val ar = Array.ofDim[Long](length)
        for{i <- 0 until length}{
          ar(i) = BytePackingUtils.unencodeLong((i*8) + 4, bytes)
        }

        GenericLongBasedID(
          // works as the default is 0 for randoms, would not work for extends then header ^ typeMask == 0 would be needed
          idTypeOf(header), ar)

      case GuaranteedUniqueHeader =>
        val maclen = 6
        // (mac: Array[Byte], ms: Long, partition: Int, rowid: Long)
        val mac = Array.ofDim[Byte](maclen)
        mac(0) = bytes(1)
        mac(1) = bytes(2)
        mac(2) = bytes(3)
        mac(3) = bytes(4)
        mac(4) = bytes(5)
        mac(5) = bytes(6)

        // dumpMap(mac, "model.parseID")

        val partition = BytePackingUtils.unencodeInt(maclen + 1, bytes)

        val msstart = (maclen + 3)
        bytes(msstart) = 0
        bytes(msstart + 1) = 0
        val msWith7 = // next 48 are in, so take first 2 from above and shift by 2 bytes
          BytePackingUtils.unencodeLong(msstart, bytes)

        // the ms is only 41 bits, however we've got an extra 7 on the end we should drop
        val ms = msWith7 >> 7

        val secondLong = // same trick to drop the last
          BytePackingUtils.unencodeLong(12, bytes)

        // secondLong only has the last 31 being the row
        val rowID = ( ( secondLong << 33 ) >>> 33 ).toInt

        GuaranteedUniqueID(mac, ms, partition, rowID)
      case _ =>  throw new InvalidIDType(header.toByte)
    }
  }

  def base64(bitLength: Int, base: Int, array: Array[Long]) = {
    val bytes = Array.ofDim[Byte](bitLength / 8)

    BytePackingUtils.encodeInt(base, 0, bytes)
    val ar = array
    for{i <- 0 until ar.length}{
      BytePackingUtils.encodeLong(ar(i), (i*8)+4, bytes)
    }

    Base64.getEncoder.encodeToString(bytes)
  }
}
