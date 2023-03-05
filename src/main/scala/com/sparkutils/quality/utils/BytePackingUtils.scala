package com.sparkutils.quality.utils

/**
 * Simple utilities to manage byte to long / int conversions
 */
object BytePackingUtils {
  /**
   * Encodes an int into the array at pos index (4 bytes)
   *
   * @param i
   * @param index
   * @return
   */
  def encodeInt(i: Int, index: Int, array: Array[Byte]): Unit = {
    array(0 + index) = (i >> 24).asInstanceOf[Byte]
    array(1 + index) = (i >> 16).asInstanceOf[Byte]
    array(2 + index) = (i >> 8).asInstanceOf[Byte]
    array(3 + index) = i.asInstanceOf[Byte]
  }

  /**
   * Encodes an Long into the array at pos index (8 bytes)
   *
   * @param i
   * @param index
   * @return
   */
  def encodeLong(i: Long, index: Int, array: Array[Byte]): Unit = {
    array(0 + index) = (i >> 56).asInstanceOf[Byte]
    array(1 + index) = (i >> 48).asInstanceOf[Byte]
    array(2 + index) = (i >> 40).asInstanceOf[Byte]
    array(3 + index) = (i >> 32).asInstanceOf[Byte]
    array(4 + index) = (i >> 24).asInstanceOf[Byte]
    array(5 + index) = (i >> 16).asInstanceOf[Byte]
    array(6 + index) = (i >> 8).asInstanceOf[Byte]
    array(7 + index) = i.asInstanceOf[Byte]
  }

  def unencodeInt(index: Int, array: Array[Byte]): Int =
    ((array(0 + index) & 0xFF) << 24) |
      ((array(1 + index) & 0xFF) << 16) |
      ((array(2 + index) & 0xFF) << 8) |
      ((array(3 + index) & 0xFF) << 0)

  def unencodeLong(index: Int, array: Array[Byte]): Long =
    ((array(0 + index) & 0xFF).asInstanceOf[Long] << 56) |
      ((array(1 + index) & 0xFF).asInstanceOf[Long] << 48) |
      ((array(2 + index) & 0xFF).asInstanceOf[Long] << 40) |
      ((array(3 + index) & 0xFF).asInstanceOf[Long] << 32) |
      ((array(4 + index) & 0xFF).asInstanceOf[Long] << 24) |
      ((array(5 + index) & 0xFF).asInstanceOf[Long] << 16) |
      ((array(6 + index) & 0xFF).asInstanceOf[Long] << 8) |
      ((array(7 + index) & 0xFF).asInstanceOf[Long] << 0)
}