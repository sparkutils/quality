package com.sparkutils.quality.impl.bloom.parquet

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.IntBuffer

import com.sparkutils.quality.utils.TransientHolder
import com.sparkutils.quality.BloomLookup

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

object BlockSplitBloomFilter {
  import com.sparkutils.quality.impl.bloom.parquet.BlockSplitBloomFilterImpl.{BITS_PER_BLOCK, LOWER_BOUND_BYTES, UPPER_BOUND_BYTES}

  /**
   * Calculate optimal size according to the number of distinct values and false positive probability.
   *
   * @param n : The number of distinct values.
   * @param p : The false positive probability.
   * @return optimal number of bits of given n and p.
   */
  def optimalNumOfBits(n: Long, p: Double): Int = {
    require(p > 0.0 && p < 1.0, "FPP should be less than 1.0 and great than 0.0")
    val m = -8 * n / Math.log(1 - Math.pow(p, 1.0 / 8))
    var numBits = m.toInt // 2147483647
    // Handle overflow.
    if (numBits > (UPPER_BOUND_BYTES << 3) || m < 0) {
      numBits = UPPER_BOUND_BYTES << 3 // 1073741824
    }

    // Round numBits up to (k * BITS_PER_BLOCK)
    numBits = (numBits + BITS_PER_BLOCK - 1) & ~BITS_PER_BLOCK // 1073742079

    if (numBits < (LOWER_BOUND_BYTES << 3)) {
      numBits = LOWER_BOUND_BYTES << 3
    }
    numBits
  }

  def optimalNumberOfBuckets(n: Long, p: Double): Long = {

    def directNumberOfBits(n: Long, p: Double): Long = {
      require(p > 0.0 && p < 1.0, "FPP should be less than 1.0 and great than 0.0")
      val m = (-8L * n).toDouble / Math.log(1L - Math.pow(p, 1.0 / 8d))
      m.toLong
    }

    val origBytes = directNumberOfBits(n, p) / 8 // bytes
    var numBytes = optimalNumOfBits(n, p) / 8 // 134217759
    if ((numBytes & (numBytes - 1)) != 0) numBytes = Integer.highestOneBit(numBytes) << 1 // 268435456
    if (numBytes > UPPER_BOUND_BYTES || numBytes < 0) numBytes = UPPER_BOUND_BYTES    // 134217728 -- is num bytes so we want prev?

    // numBytes is now likely UPPER_BOUND_BYTES anyway to use buckets - but how may bytes off are we?
    if (numBytes > origBytes)
      (numBytes.toDouble / origBytes.toDouble).ceil.toLong
    else
      (origBytes.toDouble / numBytes.toDouble).ceil.toLong
  }

}

/*
 * This Bloom filter is implemented using block-based Bloom filter algorithm from Putze et al.'s
 * "Cache-, Hash- and Space-Efficient Bloom filters". The basic idea is to hash the item to a tiny
 * Bloom filter which size fit a single cache line or smaller. This implementation sets 8 bits in
 * each tiny Bloom filter. Each tiny Bloom filter is 32 bytes to take advantage of 32-byte SIMD
 * instruction.
 */
object BlockSplitBloomFilterImpl { // Bytes in a tiny Bloom filter block.
  private[parquet] val BYTES_PER_BLOCK = 32
  // Bits in a tiny Bloom filter block.
  private[parquet] val BITS_PER_BLOCK = 256
  // The lower bound of bloom filter size, set to the size of a tiny Bloom filter block.
  val LOWER_BOUND_BYTES = 32
  // The upper bound of bloom filter size, set to default row group size.
  val UPPER_BOUND_BYTES: Int = 128 * 1024 * 1024
  // The number of bits to set in a tiny Bloom filter
  private[parquet] val BITS_SET_PER_BLOCK = 8
  // The metadata in the header of a serialized Bloom filter is four four-byte values: the number of bytes,
  // the filter algorithm, the hash algorithm, and the compression.
  val HEADER_SIZE = 16
  // The default false positive probability value
  val DEFAULT_FPP = 0.01
  // The block-based algorithm needs 8 odd SALT values to calculate eight indexes
  // of bits to set, one per 32-bit word.
  private[parquet] val SALT = Array(0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d, 0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31)

  def fromBytes(bitset: Array[Byte]): BlockSplitBloomFilterImpl = {
    apply(bitset)
  }

  /**
   * Construct the Bloom filter with given bitset, it is used when reconstructing
   * Bloom filter from parquet file. It use XXH64 as its default hash
   * function.
   *
   * @param bitset The given bitset to construct Bloom filter.
   */
  def apply(bitset: Array[Byte]): BlockSplitBloomFilterImpl = {
    apply(bitset, BloomFilter.XXH64)
  }

  def arrayBacked(bits: Array[Byte]): TransientHolder[BufferAndRaw] = {

    val ibuffer = TransientHolder{ () =>
      val buf = ByteBuffer.wrap(bits).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer
      BufferAndRaw(buf, Some(bits))
    }
    ibuffer
  }


  /**
   * Construct the Bloom filter with given bitset, it is used when reconstructing
   * Bloom filter from parquet file.
   *
   * @param bitset       The given bitset to construct Bloom filter.
   * @param hashImpl A hashImpl, no attempt at thread safety is made here, the caller must ensure thread safety
   */
  def apply(bitset: Array[Byte], hashImpl: BloomHash): BlockSplitBloomFilterImpl = {

    val bs = BlockSplitBloomFilterImpl(arrayBacked(bitset), hashImpl)

    bs
  }

  /**
   * Construct the Bloom filter with given bitset, it is used when reconstructing
   * Bloom filter from parquet file.
   *
   * @param bitset       The given bitset to construct Bloom filter.
   * @param hashStrategy The hash strategy Bloom filter apply.
   */
  def apply(bitset: Array[Byte], hashStrategy: BloomFilter.HashStrategy): BlockSplitBloomFilterImpl =
    apply(bitset,  new BloomHashImpl(hashStrategy))


  def bufferEquals(left: IntBuffer, right: IntBuffer) =
    if (left.limit() == right.limit()) {
      var i = -1
      do {
        i += 1
      } while( i < right.limit() && left.get(i) == right.get(i) )
      if ((i+1) == right.limit())
        true // got to the end
      else
        false // did not there is a difference
    } else
      false

  /**
   * Creates using an array
   *
   * @param numBytes
   * @param iMinimumBytes
   * @param iMaximumBytes
   * @param hashImpl
   * @return
   */
  def apply(numBytes: Int, iMinimumBytes: Int, iMaximumBytes: Int, hashImpl: BloomHash): BlockSplitBloomFilterImpl =
    apply(createArray(numBytes, iMinimumBytes, iMaximumBytes), hashImpl)

  def createArray(numBytes: Int, iMinimumBytes: Int, iMaximumBytes: Int) = new Array[Byte]({


    val maximumBytes =
      if (iMaximumBytes > BlockSplitBloomFilterImpl.LOWER_BOUND_BYTES && iMaximumBytes < BlockSplitBloomFilterImpl.UPPER_BOUND_BYTES)
        iMaximumBytes
      else
        BlockSplitBloomFilterImpl.UPPER_BOUND_BYTES
    val minimumBytes =
      if (iMinimumBytes > BlockSplitBloomFilterImpl.LOWER_BOUND_BYTES && iMinimumBytes < BlockSplitBloomFilterImpl.UPPER_BOUND_BYTES)
        iMaximumBytes
      else
        BlockSplitBloomFilterImpl.LOWER_BOUND_BYTES

    var inumBytes = numBytes
    if (inumBytes < minimumBytes) {
      inumBytes = minimumBytes
    }
    // Get next power of 2 if it is not power of 2.
    if ((inumBytes & (inumBytes - 1)) != 0) inumBytes = Integer.highestOneBit(inumBytes) << 1
    if (inumBytes > maximumBytes || inumBytes < 0) inumBytes = maximumBytes

    inumBytes
  })

}

case class BufferAndRaw(buffer: IntBuffer, bytes: Option[Array[Byte]] = None)

/**
 * Constructor of block-based Bloom filter.
 *
 * @param intBufferGen Creates the backing buffer
 * @param hashImpl Manages hashing for this bloomfilter
 */
case class BlockSplitBloomFilterImpl(intBufferGen: TransientHolder[BufferAndRaw], hashImpl: BloomHash) extends
  BloomFilter[Array[Byte]] with BloomLookupImpl with Bloom[Array[Byte]] with DelegatingBloomHash { // The hash strategy used in this Bloom filter.
  type BloomType = BlockSplitBloomFilterImpl

  /*
   * @param iMinimumBytes The minimum bytes of the Bloom filter.
 * @param iMaximumBytes The maximum bytes of the Bloom filter.

   */
//  require(iMinimumBytes <= iMaximumBytes, "the minimum bytes should be less or equal than maximum bytes")
  require(hashStrategy == BloomFilter.XXH64, s"Unsupported hash strategy $hashStrategy")

  cacheBuffer.order(ByteOrder.LITTLE_ENDIAN)

  override def intBuffer: IntBuffer = intBufferGen.get().buffer

  /**
   * Constructor of block-based Bloom filter backed by an array of the correct size
   *
   * @param numBytes The number of bytes for Bloom filter bitset. The range of num_bytes should be within
   *                 [DEFAULT_MINIMUM_BYTES, DEFAULT_MAXIMUM_BYTES], it will be rounded up/down
   *                 to lower/upper bound if num_bytes is out of range. It will also be rounded up to a power
   *                 of 2. It uses XXH64 as its default hash function.
   */
  def this(numBytes: Int) {
    this({
      val bits = BlockSplitBloomFilterImpl.createArray(numBytes, BlockSplitBloomFilterImpl.LOWER_BOUND_BYTES, BlockSplitBloomFilterImpl.UPPER_BOUND_BYTES)
      BlockSplitBloomFilterImpl.arrayBacked(bits)
    }, new BloomHashImpl(BloomFilter.XXH64))
  }

  def insertHash(hash: Long): Unit = {
    val numBlocks = getBitsetSize / BlockSplitBloomFilterImpl.BYTES_PER_BLOCK
    val lowHash = hash >>> 32
    val blockIndex = ((lowHash * numBlocks) >> 32).toInt
    val key = hash.toInt
    // Calculate mask for bucket.
    val mask = setMask(key)
    for (i <- 0 until BlockSplitBloomFilterImpl.BITS_SET_PER_BLOCK) {
      var value = intBuffer.get(blockIndex * (BlockSplitBloomFilterImpl.BYTES_PER_BLOCK / 4) + i)
      value |= mask(i)
      intBuffer.put(blockIndex * (BlockSplitBloomFilterImpl.BYTES_PER_BLOCK / 4) + i, value)
    }
  }

  /** CTw added the or'ing */
  override def |= (that: BlockSplitBloomFilterImpl): BlockSplitBloomFilterImpl = {
    checkCompatibility(that)
    // Breeze copies but we'll do in place
    for(i <- 0 until intBuffer.limit()) {
      intBuffer.put(i, intBuffer.get(i) | that.intBuffer.get(i))
    }
    this
  }

  private def checkCompatibility(that: BloomFilter[Array[Byte]]): Unit =
    require(that.getBitsetSize == this.getBitsetSize, "Must have the same number of buckets to intersect")

  def getBitsetSize: Int = this.intBuffer.limit() * 4

  override def equals(other: Any): Boolean =
    other match {
      case bs: BlockSplitBloomFilterImpl if bs eq this => true
      case that: BlockSplitBloomFilterImpl =>
        BlockSplitBloomFilterImpl.bufferEquals(this.intBuffer, that.intBuffer) && (this.getAlgorithm eq that.getAlgorithm) && (this.hashStrategy eq that.hashStrategy)
      case _ => false
    }

  def getHashStrategy: BloomFilter.HashStrategy = BloomFilter.XXH64

  def getAlgorithm: BloomFilter.Algorithm = BloomFilter.BLOCK

  def getCompression: BloomFilter.Compression = BloomFilter.UNCOMPRESSED

  def hash(value: Int): Long = {
    cacheBuffer.putInt(value)
    doHash
  }

  def hash(value: Long): Long = {
    cacheBuffer.putLong(value)
    doHash
  }

  def hash(value: Double): Long = {
    cacheBuffer.putDouble(value)
    doHash
  }

  def hash(value: Float): Long = {
    cacheBuffer.putFloat(value)
    doHash
  }

  def hash(value: BloomFilter.Binary): Long = hashFunction.hashBytes(value)

  override def serialized: Array[Byte] = {
    val a = Array.ofDim[Byte](getBitsetSize)
    val copy = ByteBuffer.wrap(a).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer()
    copy.put(intBuffer)
      // now in a
    a
  }

}