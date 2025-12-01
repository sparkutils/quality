package com.sparkutils.quality.impl.hash

import com.google.common.hash.{HashFunction, Hasher, Hashing}
import com.sparkutils.quality.impl.util.{BytePackingUtils, TSLocal}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.shim.hash.{Digest, DigestFactory, HashLongsExpression}
import org.apache.spark.sql.types.DataType

/**
 * Proxies HashFunction for this hash
 * @param hasher
 */
case class HashFunctionProxy(hasher: Hasher) extends Digest {

  override def hashInt(i: Int): Unit =
    hasher.putInt(i)

  override def hashLong(l: Long): Unit =
    hasher.putLong(l)

  override def hashBytes(base: Array[Byte], offset: Int, length: Int): Unit =
    hasher.putBytes(base, offset, length)

  override def digest: Array[Long] = {
    val bytes = hasher.hash.asBytes
    val res = Array.ofDim[Long]((bytes.length + 7) / 8) // next whole long size up

    // to make sure the array is large enough pad as needed, so a 32bit digest will always take 64
    val use =
      if (bytes.length / 8 != res.length) {
        val nbytes = Array.ofDim[Byte](res.length * 8)
        Array.copy(bytes, 0, nbytes, 0, bytes.length)
        nbytes
      } else
        bytes

    for{ i <- 0 until res.length} {
      res(i) = BytePackingUtils.unencodeLong(i * 8, use)
    }

    res
  }
}

// spark 3.1.2 has 11.0.2 3.0.3 has 16.0.1
object HashFunctionFactory {
  val mapF = Map[String, () => HashFunction](
    ("MURMUR3_32", () => Hashing.murmur3_32),
    ("MURMUR3_128", () => Hashing.murmur3_128),
    ("MD5", () => Hashing.md5),
    ("SHA-1", () => Hashing.sha1),
    ("SHA-256", () => Hashing.sha256),
    ("SHA-512", () => Hashing.sha512),
    ("ADLER32", () => Hashing.adler32),
    ("CRC32", () => Hashing.crc32),
    ("SIPHASH24", () => Hashing.sipHash24)
  )
}

/**
 * Creates a HashFunction / Hasher for a given impl - defaults to Murmur3 128 when no matching impl is found
 * @param impl
 */
case class HashFunctionFactory(impl: String) extends DigestFactory {
  val mapf = HashFunctionFactory.mapF.getOrElse(impl, HashFunctionFactory.mapF("MURMUR3_128"))
  private val ts = TSLocal[HashFunction]{
    mapf
  }
  override def fresh: Digest = HashFunctionProxy( ts.get.newHasher )

  override def length: Int =
    (ts.get.bits + 63) / 64 // next whole long size up
}

case class HashFunctionsExpression(children: Seq[Expression], digestImpl: String, asStruct: Boolean, factory: DigestFactory) extends HashLongsExpression {

  override def prettyName: String = digestImpl

  override protected def hasherClassName: String = DigestLongsFunction.getClass.getName

  override protected def computeHash(value: Any, dataType: DataType, hash: Digest): Unit =
    DigestLongsFunction.hash(value, dataType, hash)

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(children = newChildren)
}
