package com.sparkutils.quality.impl.bloom.parquet

import java.io.IOException
import java.io.OutputStream
import java.nio.{ByteBuffer, ByteOrder, IntBuffer}

import com.sparkutils.quality.impl.rng.RandomLongs
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.types.{ArrayType, BinaryType, DoubleType, IntegerType, LongType, StringType}

import scala.collection.mutable

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


/**
 * A Bloom filter is a compact structure to indicate whether an item is not in a set or probably
 * in a set. The Bloom filter usually consists of a bit set that represents a elements set,
 * a hash strategy and a Bloom filter algorithm.
 */
object BloomFilter {

  type Binary = Array[Byte]

  /* Bloom filter Hash strategy.
    *
    * xxHash is an extremely fast hash algorithm, running at RAM speed limits. It successfully
    * completes the SMHasher test suite which evaluates collision, dispersion and randomness qualities
    * of hash functions. It shows good performance advantage from its benchmark result.
    * (see https://github.com/Cyan4973/xxHash).
    */
  sealed trait HashStrategy
  case object XXH64 extends HashStrategy {
    override def toString = "xxhash"
  }

  // Bloom filter algorithm.
  sealed trait Algorithm
  case object BLOCK extends Algorithm {
    override def toString = "block"
  }

  // Bloom filter compression.
  sealed trait Compression
  case object UNCOMPRESSED extends Compression {
    override def toString = "uncompressed"
  }

}

trait Bloom[SerializedType] extends Serializable {
  type BloomType <: Bloom[SerializedType]

  def += (value: Any): BloomType

  def serialized: Array[Byte]

  def |= (that: BloomType): BloomType

}

trait BloomFilter[SerializedType] extends Serializable with Bloom[SerializedType] {
  type BloomType <: BloomFilter[SerializedType]

  /**
   * Insert an element to the Bloom filter, the element content is represented by
   * the hash value of its plain encoding result.
   *
   * @param hash the hash result of element.
   */
  def insertHash(hash: Long): Unit

  /**
   * Determine whether an element is in set or not.
   *
   * @param hash the hash value of element plain encoding result.
   * @return false if element is must not in set, true if element probably in set.
   */
  def findHash(hash: Long): Boolean

  /**
   * Get the number of bytes for bitset in this Bloom filter.
   *
   * @return The number of bytes for bitset in this Bloom filter.
   */
  def getBitsetSize: Int

  /**
   * Compute hash for int value by using its plain encoding result.
   *
   * @param value the value to hash
   * @return hash result
   */
  def hash(value: Int): Long

  /**
   * Compute hash for long value by using its plain encoding result.
   *
   * @param value the value to hash
   * @return hash result
   */
  def hash(value: Long): Long

  /**
   * Compute hash for double value by using its plain encoding result.
   *
   * @param value the value to hash
   * @return hash result
   */
  def hash(value: Double): Long

  /**
   * Compute hash for float value by using its plain encoding result.
   *
   * @param value the value to hash
   * @return hash result
   */
  def hash(value: Float): Long

  /**
   * Compute hash for Binary value by using its plain encoding result.
   *
   * @param value the value to hash
   * @return hash result
   */
  def hash(value: BloomFilter.Binary): Long

  /**
   * Compute hash for Object value by using its plain encoding result.
   *
   * @param value the value to hash
   * @return hash result
   */
  def hash(value: Any): Long

  /**
   * Return the hash strategy that the bloom filter apply.
   *
   * @return hash strategy that the bloom filter apply
   */
  def getHashStrategy: BloomFilter.HashStrategy

  /**
   * Return the algorithm that the bloom filter apply.
   *
   * @return algorithm that the bloom filter apply
   */
  def getAlgorithm: BloomFilter.Algorithm

  /**
   * Return the compress algorithm that the bloom filter apply.
   *
   * @return compress algorithm that the bloom filter apply
   */
  def getCompression: BloomFilter.Compression

  override def += (value: Any): BloomType = {
    val h = hash(value)
    insertHash(h)
    this.asInstanceOf[BloomType]
  }
}

trait DelegatingBloomHash extends BloomHash {
  def hashImpl: BloomHash

  override def hashStrategy: BloomFilter.HashStrategy = hashImpl.hashStrategy

  override def cacheBuffer: ByteBuffer = hashImpl.cacheBuffer

  override def mask: Array[Int] = hashImpl.mask
}

object BloomHash {
  val hashTypes = Seq(RandomLongs.structType, BinaryType, StringType, LongType, IntegerType, DoubleType, ArrayType(LongType))
}

private[bloom] trait BloomHash extends Serializable {
  def hashStrategy: BloomFilter.HashStrategy

  val hashFunction =
    hashStrategy match {
      case BloomFilter.XXH64 =>
        new XxHash()
      case _ =>
        throw new RuntimeException("Unsupported hash strategy")
    }

  def cacheBuffer: ByteBuffer
  def mask: Array[Int]

  def setMask(key: Int) = { // The following three loops are written separately so that they could be
    // optimized for vectorization.
    for (i <- 0 until BlockSplitBloomFilterImpl.BITS_SET_PER_BLOCK) {
      mask(i) = key * BlockSplitBloomFilterImpl.SALT(i)
    }
    for (i <- 0 until BlockSplitBloomFilterImpl.BITS_SET_PER_BLOCK) {
      mask(i) = mask(i) >>> 27
    }
    for (i <- 0 until BlockSplitBloomFilterImpl.BITS_SET_PER_BLOCK) {
      mask(i) = 0x1 << mask(i)
    }
    mask
  }

  def hash(value: Any): Long = {
    value match {
      case b: BloomFilter.Binary => return hashFunction.hashBytes(b)
      case wa: mutable.WrappedArray[_] =>
        val ar = wa.asInstanceOf[mutable.WrappedArray[Long]]
        return hashFunction.hashLongs(ar.toArray)
      case la: Array[Long] => return hashFunction.hashLongs(la)
      case i: Integer => cacheBuffer.putInt(i)
      case l: Long => cacheBuffer.putLong(l)
      case f: Float => cacheBuffer.putFloat(f)
      case d: Double => cacheBuffer.putDouble(d)
      case s: String => return hashFunction.hashChars(s)
      case _ => throw new RuntimeException("Parquet Bloom filter: Not supported type")
    }
    doHash
  }

  def doHash = {
    cacheBuffer.flip
    val hashResult = hashFunction.hashByteBuffer(cacheBuffer)
    cacheBuffer.clear
    hashResult
  }

}

class BloomHashImpl(val hashStrategy: BloomFilter.HashStrategy) extends BloomHash {
  override val cacheBuffer: ByteBuffer = {
    val res = ByteBuffer.allocate(8)
    res.order(ByteOrder.LITTLE_ENDIAN)
    res
  }

  override val mask: Array[Int] =
    new Array[Int](BlockSplitBloomFilterImpl.BITS_SET_PER_BLOCK)
}

private[bloom] trait BloomLookupImpl extends Serializable with com.sparkutils.quality.BloomLookup with BloomHash {
  def intBuffer: IntBuffer

  def findHash(hash: Long): Boolean = {
    val numBlocks = (intBuffer.limit() * 4) / BlockSplitBloomFilterImpl.BYTES_PER_BLOCK
    val lowHash = hash >>> 32
    val blockIndex = ((lowHash * numBlocks) >> 32).toInt
    val key = hash.toInt
    // Calculate mask for the tiny Bloom filter.
    val mask = setMask(key)
    for (i <- 0 until BlockSplitBloomFilterImpl.BITS_SET_PER_BLOCK) {
      if (0 == (intBuffer.get(blockIndex * (BlockSplitBloomFilterImpl.BYTES_PER_BLOCK / 4) + i) & mask(i))) return false
    }
    true
  }

  def mightContain( value : Any) = {
    val h = hash(value)
    findHash(h)
  }

}