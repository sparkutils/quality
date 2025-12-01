package com.sparkutils.quality.impl.bloom.parquet

import java.nio.ByteBuffer


/**
 * A interface contains a set of hash functions used by Bloom filter.
 */
trait HashFunction {
  /**
   * compute the hash value for a byte array.
   *
   * @param input the input byte array
   * @return a result of long value.
   */
  def hashBytes(input: Array[Byte]): Long

  /**
   * compute the hash value for a Long array.
   *
   * @param input the input Long array
   * @return a result of long value.
   */
  def hashLongs(input: Array[Long]): Long

  /**
   * compute the hash value for a ByteBuffer.
   *
   * @param input the input ByteBuffer
   * @return a result of long value.
   */
  def hashByteBuffer(input: ByteBuffer): Long

  /**
   * compute the hash value for a String
   *
   * @param input
   * @return a result of long value
   */
  def hashChars(input: String): Long
}