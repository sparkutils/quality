package com.sparkutils.quality.impl.hash

import org.apache.spark.sql.Column
import org.apache.spark.sql.ShimUtils.callFunction
import org.apache.spark.sql.functions.lit

trait HashRelatedFunctionImports {

  protected def hashF(func: String, digestImpl: String, cols: Column*): Column =
    callFunction(func, (Seq(lit(digestImpl)) ++ cols) :_*)

  /**
   * Converts columns into a digest via the MessageDigest digestImpl
   *
   * @param digestImpl
   * @param cols
   * @return array of long
   */
  def digest_to_longs(digestImpl: String, cols: Column*): Column =
    hashF("digest_to_longs", digestImpl, cols: _*)

  /**
   * Converts columns into a digest via the MessageDigest digestImpl
   *
   * @param digestImpl
   * @param cols
   * @return struct with fields i0, i1, i2 etc.
   */
  def digest_to_longs_struct(digestImpl: String, cols: Column*): Column =
    hashF("digest_to_longs_struct", digestImpl, cols: _*)

  protected def fieldBasedIDF(func: String, prefix: String, digestImpl: String, cols: Column*) =
    callFunction(func, (Seq(lit(prefix), lit(digestImpl)) ++ cols) :_*)

  /**
   * Creates an id from fields using MessageDigests
   * @param prefix
   * @param digestImpl
   * @param cols
   * @return
   */
  def field_based_id(prefix: String, digestImpl: String, cols: Column*): Column =
    fieldBasedIDF("field_based_id", prefix, digestImpl, cols :_*)


  /**
   * Creates an id from fields using MessageDigests, in line with SQL naming please use field_based_id
   *
   * @param prefix
   * @param digestImpl
   * @param children
   * @return
   */
  @deprecated(since = "0.1.0", message = "migrate to field_based_id")
  def fieldBasedID(prefix: String, digestImpl: String, children: Column *): Column =
    field_based_id(prefix, digestImpl, children:_*)

  /**
   * Creates an id from fields using ZeroAllocation LongTuple Factory (128-bit)
   *
   * @param prefix
   * @param digestImpl
   * @param cols
   * @return
   */
  def za_longs_field_based_id(prefix: String, digestImpl: String, cols: Column*): Column =
    fieldBasedIDF("za_longs_field_based_id", prefix, digestImpl, cols :_*)

  /**
   * Creates an id from fields using ZeroAllocation LongHashFactory (64bit)
   *
   * @param prefix
   * @param digestImpl
   * @param cols
   * @return
   */
  def za_field_based_id(prefix: String, digestImpl: String, cols: Column*): Column =
    fieldBasedIDF("za_field_based_id", prefix, digestImpl, cols :_*)

  /**
   * Creates an id from fields using Guava Hashers
   *
   * @param prefix
   * @param digestImpl
   * @param cols
   * @return
   */
  def hash_field_based_id(prefix: String, digestImpl: String, cols: Column*): Column =
    fieldBasedIDF("hash_field_based_id", prefix, digestImpl, cols :_*)

  /**
   * Converts columns into a digest using Guava Hashers
   *
   * @param digestImpl
   * @param cols
   * @return array of long
   */
  def hash_with(digestImpl: String, cols: Column*): Column =
    hashF("hash_with", digestImpl, cols: _*)

  /**
   * Converts columns into a digest using Guava Hashers
   *
   * @param digestImpl
   * @param cols
   * @return struct with fields i0, i1, i2 etc.
   */
  def hash_with_struct(digestImpl: String, cols: Column*): Column =
    hashF("hash_with_struct", digestImpl, cols: _*)

  /**
   * Converts columns into a digest via ZeroAllocation LongHashFactory (64bit)
   *
   * @param digestImpl
   * @param cols
   * @return array of long
   */
  def za_hash_with(digestImpl: String, cols: Column*): Column =
    hashF("za_hash_with", digestImpl, cols: _*)

  /**
   * Converts columns into a digest via ZeroAllocation LongHashFactory (64bit)
   *
   * @param digestImpl
   * @param cols
   * @return struct with fields i0, i1, i2 etc.
   */
  def za_hash_with_struct(digestImpl: String, cols: Column*): Column =
    hashF("za_hash_with_struct", digestImpl, cols: _*)

  /**
   * Converts columns into a digest via ZeroAllocation LongTuple Factory (128-bit)
   *
   * @param digestImpl
   * @param cols
   * @return array of long
   */
  def za_hash_longs_with(digestImpl: String, cols: Column*): Column =
    hashF("za_hash_longs_with", digestImpl, cols: _*)

  /**
   * Converts columns into a digest via ZeroAllocation LongTuple Factory (128-bit)
   *
   * @param digestImpl
   * @param cols
   * @return struct with fields i0, i1, i2 etc.
   */
  def za_hash_longs_with_struct(digestImpl: String, cols: Column*): Column =
    hashF("za_hash_longs_with_struct", digestImpl, cols: _*)

}
