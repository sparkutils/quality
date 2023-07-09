package com.sparkutils.quality.impl.hash

import com.sparkutils.quality.impl.util.{BytePackingUtils, TSLocal}
import net.openhft.hashing.{LongHashFunction, LongTupleHashFunction}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.qualityFunctions.{Digest, DigestFactory, HashLongsExpression}
import org.apache.spark.sql.types.DataType

/**
 * Proxies LongHashFunction for this hash, clearly it actually allocates as we use the last result to seed a new hasher
 * @param hasher
 */
case class LongHashFunctionProxy(hasher: Long => LongHashFunction) extends Digest {
  private var long = 0L

  override def hashInt(i: Int): Unit =
    long = hasher(long).hashInt(i)

  override def hashLong(l: Long): Unit =
    long = hasher(long).hashLong(l)

  override def hashBytes(base: Array[Byte], offset: Int, length: Int): Unit =
    long = hasher(long).hashBytes(base, offset, length)

  override def digest: Array[Long] = {
    val res = Array.ofDim[Long](1) // always one long here

    res(0) = long

    res
  }
}

object LongHashFunctionFactory {
  val mapF = Map[String, () => Long => LongHashFunction](
    ("MURMUR3_64", () => LongHashFunction.murmur_3(_)),
    ("CITY_1_1", () => LongHashFunction.city_1_1(_)),
    ("FARMNA", () => LongHashFunction.farmNa(_)),
    ("FARMOU", () => LongHashFunction.farmUo(_)),
    ("METRO", () => LongHashFunction.metro(_)),
    ("WY_V3", () => LongHashFunction.wy_3(_)),
    ("XX", () => LongHashFunction.xx(_))
  )
}

/**
 * Creates a LongHashFunction / Hasher for a given ZeroAllocation impl - defaults to XX when no matching impl is found
 * @param impl
 */
case class ZALongHashFunctionFactory(impl: String) extends DigestFactory {
  val mapf = LongHashFunctionFactory.mapF.getOrElse(impl, LongHashFunctionFactory.mapF("XX"))
  private val ts = TSLocal[Long => LongHashFunction]{
    mapf
  }
  override def fresh: Digest = LongHashFunctionProxy( ts.get() )

  override def length: Int = 1
}

/**
 * Proxies LongTupleHashFunction for this hash, uses the last result
 * @param hasher
 */
case class LongTupleHashFunctionProxy(hasher: Long => LongTupleHashFunction) extends Digest {
  private val buffer = Array.ofDim[Long](2)

  override def hashInt(i: Int): Unit =
    hasher(buffer(0)).hashInt(i, buffer)

  override def hashLong(l: Long): Unit =
    hasher(buffer(0)).hashLong(l, buffer)

  override def hashBytes(base: Array[Byte], offset: Int, length: Int): Unit =
    hasher(buffer(0)).hashBytes(base, offset, length, buffer)

  override def digest: Array[Long] = {
    val res = Array.ofDim[Long](2) // always one long here

    res(0) = buffer(0)
    res(1) = buffer(1)

    res
  }
}


object LongTupleHashFunctionFactory {
  val mapF = Map[String, () => Long => LongTupleHashFunction](
    ("MURMUR3_128", () => LongTupleHashFunction.murmur_3(_)),
    ("XXH3", () => LongTupleHashFunction.xx128(_))
  )
}

/**
 * Creates a LongHashFunction / Hasher for a given ZeroAllocation impl - XXH3 as default
 * @param impl
 */
case class ZALongTupleHashFunctionFactory(impl: String) extends DigestFactory {
  val mapf = LongTupleHashFunctionFactory.mapF.getOrElse(impl, LongTupleHashFunctionFactory.mapF("XXH3"))
  private val ts = TSLocal[Long => LongTupleHashFunction]{
    mapf
  }
  override def fresh: Digest = LongTupleHashFunctionProxy( ts.get() )

  override def length: Int = (ts.get()(0).bitsLength / 64)
}
