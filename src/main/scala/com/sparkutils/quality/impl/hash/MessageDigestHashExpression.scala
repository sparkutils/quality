package com.sparkutils.quality.impl.hash

import java.security.MessageDigest

import com.sparkutils.quality.utils.BytePackingUtils
import com.sparkutils.quality.utils.{BytePackingUtils, TSLocal}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.qualityFunctions.{Digest, DigestFactory, HashLongsExpression, InterpretedHashLongsFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}

/**
 * Proxies MessageDigest for this hash
 * @param messageDigest
 */
case class MessageDigestProxy(messageDigest: MessageDigest) extends Digest {
  private val buffer = Array.ofDim[Byte](8) // a long

  override def hashInt(i: Int): Unit = {
    BytePackingUtils.encodeInt(i, 0, buffer)
    messageDigest.update(buffer, 0, 4)
  }

  override def hashLong(l: Long): Unit = {
    BytePackingUtils.encodeLong(l, 0, buffer)
    messageDigest.update(buffer, 0, 8)
  }

  override def hashBytes(base: Array[Byte], offset: Int, length: Int): Unit =
    messageDigest.update(base, offset, length)

  override def digest: Array[Long] = {
    val bytes = messageDigest.digest
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

/**
 * Calling java.security.MessageDigest.digest resets the digest so we only need create one instance and just return it
 * @param impl
 */
case class MessageDigestFactory(impl: String) extends DigestFactory {
  private val ts = TSLocal[MessageDigest]{
    () => MessageDigest.getInstance(impl)
  }
  override def fresh: Digest = MessageDigestProxy( ts.get )

  override def length: Int =
    (ts.get.getDigestLength + 7) / 8 // next whole long size up
}
/*
case class MessageDigestLongs(children: Seq[Expression], digestImpl: String, asStruct: Boolean) extends HashLongsExpression {

  override def prettyName: String = digestImpl

  override protected def hasherClassName: String = DigestLongsFunction.getClass.getName

  override protected def computeHash(value: Any, dataType: DataType, hash: Digest): Unit =
    DigestLongsFunction.hash(value, dataType, hash)

  override val factory: DigestFactory = MessageDigestFactory(digestImpl)

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(children = newChildren)
}
*/
object DigestLongsFunction extends InterpretedHashLongsFunction {
  override def hashInt(i: Int, digest: Digest): Digest = {
    digest.hashInt(i)
    digest
  }

  override def hashLong(l: Long, digest: Digest): Digest = {
    digest.hashLong(l)
    digest
  }

  override def hashBytes(base: Array[Byte], offset: Int, length: Int, digest: Digest): Digest = {
    digest.hashBytes(base, offset, length)
    digest
  }
}
