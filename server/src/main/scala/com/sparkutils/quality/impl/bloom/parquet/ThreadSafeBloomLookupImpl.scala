package com.sparkutils.quality.impl.bloom.parquet

import com.sparkutils.quality.impl.util.TSLocal

import java.nio.{ByteBuffer, ByteOrder, IntBuffer}

case class ThreadLookup(bitset: Array[Byte], hashImpl: BloomHash) extends BloomLookupImpl with DelegatingBloomHash {

  override val intBuffer: IntBuffer =
    ByteBuffer.wrap(bitset).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer

}

case class ThreadBufferLookup(intBuffer: IntBuffer, hashImpl: BloomHash) extends BloomLookupImpl with DelegatingBloomHash

/**
 * The default BlockSplitBloomFilter is not threadsafe for lookup.
 * This implementation uses a thread local hash preparation for lookup
 *
 * @param bitset the bloom
 */
case class ThreadSafeBloomLookupImpl(bitset: Array[Byte], hashStrategy: BloomFilter.HashStrategy = BloomFilter.XXH64) extends com.sparkutils.quality.BloomLookup {

  private val impl: TSLocal[ThreadLookup] = TSLocal[ThreadLookup]{ () =>
    ThreadLookup(bitset, new BloomHashImpl(hashStrategy))
  }

  override def mightContain(value: Any): Boolean = impl.get().mightContain(value)
}
