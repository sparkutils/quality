package com.sparkutils.quality.impl.hash

import com.sparkutils.quality.impl.id.{GenericLongBasedIDExpression, model}
import org.apache.spark.sql.Column
import org.apache.spark.sql.ShimUtils.{column, expression}
import org.apache.spark.sql.shim.hash.DigestFactory

trait HashRelatedFunctionImports {

  protected def hashF(asStruct: Boolean, digestImpl: String, factory: String => DigestFactory, cols: Column*): Column =
    column(HashFunctionsExpression(cols.map(expression(_)), digestImpl, asStruct, factory(digestImpl)))

  /**
   * Converts columns into a digest via the MessageDigest digestImpl
   *
   * @param digestImpl
   * @param cols
   * @return array of long
   */
  def digest_to_longs(digestImpl: String, cols: Column*): Column =
    hashF(false, digestImpl, MessageDigestFactory, cols: _*)

  /**
   * Converts columns into a digest via the MessageDigest digestImpl
   *
   * @param digestImpl
   * @param cols
   * @return struct with fields i0, i1, i2 etc.
   */
  def digest_to_longs_struct(digestImpl: String, cols: Column*): Column =
    hashF(true, digestImpl, MessageDigestFactory, cols: _*)

  protected def fieldBasedIDF(prefix: String, digestImpl: String, factory: String => DigestFactory, cols: Column*): Column =
    column(GenericLongBasedIDExpression(model.FieldBasedID,
      HashFunctionsExpression(cols.map(expression(_)), digestImpl, true, factory(digestImpl)), prefix))

  /**
   * Creates an id from fields using MessageDigests
   * @param prefix
   * @param digestImpl
   * @param cols
   * @return
   */
  def field_based_id(prefix: String, digestImpl: String, cols: Column*): Column =
    fieldBasedIDF(prefix, digestImpl, MessageDigestFactory, cols: _*)

  /**
   * Creates an id from fields using ZeroAllocation LongTuple Factory (128-bit)
   *
   * @param prefix
   * @param digestImpl
   * @param cols
   * @return
   */
  def za_longs_field_based_id(prefix: String, digestImpl: String, cols: Column*): Column =
    fieldBasedIDF(prefix, digestImpl, ZALongTupleHashFunctionFactory, cols: _*)

  /**
   * Creates an id from fields using ZeroAllocation LongHashFactory (64bit)
   *
   * @param prefix
   * @param digestImpl
   * @param cols
   * @return
   */
  def za_field_based_id(prefix: String, digestImpl: String, cols: Column*): Column =
    fieldBasedIDF(prefix, digestImpl, ZALongHashFunctionFactory, cols: _*)

  /**
   * Creates an id from fields using Guava Hashers
   *
   * @param prefix
   * @param digestImpl
   * @param cols
   * @return
   */
  def hash_field_based_id(prefix: String, digestImpl: String, cols: Column*): Column =
    fieldBasedIDF(prefix, digestImpl, HashFunctionFactory(_), cols: _*)

  /**
   * Converts columns into a digest using Guava Hashers
   *
   * @param digestImpl
   * @param cols
   * @return array of long
   */
  def hash_with(digestImpl: String, cols: Column*): Column =
    hashF(false, digestImpl, HashFunctionFactory(_), cols: _*)

  /**
   * Converts columns into a digest using Guava Hashers
   *
   * @param digestImpl
   * @param cols
   * @return struct with fields i0, i1, i2 etc.
   */
  def hash_with_struct(digestImpl: String, cols: Column*): Column =
    hashF(true, digestImpl, HashFunctionFactory(_), cols: _*)

  /**
   * Converts columns into a digest via ZeroAllocation LongHashFactory (64bit)
   *
   * @param digestImpl
   * @param cols
   * @return array of long
   */
  def za_hash_with(digestImpl: String, cols: Column*): Column =
    hashF(false, digestImpl, ZALongHashFunctionFactory, cols: _*)

  /**
   * Converts columns into a digest via ZeroAllocation LongHashFactory (64bit)
   *
   * @param digestImpl
   * @param cols
   * @return struct with fields i0, i1, i2 etc.
   */
  def za_hash_with_struct(digestImpl: String, cols: Column*): Column =
    hashF(true, digestImpl, ZALongHashFunctionFactory, cols: _*)

  /**
   * Converts columns into a digest via ZeroAllocation LongTuple Factory (128-bit)
   *
   * @param digestImpl
   * @param cols
   * @return array of long
   */
  def za_hash_longs_with(digestImpl: String, cols: Column*): Column =
    hashF(false, digestImpl, ZALongTupleHashFunctionFactory, cols: _*)

  /**
   * Converts columns into a digest via ZeroAllocation LongTuple Factory (128-bit)
   *
   * @param digestImpl
   * @param cols
   * @return struct with fields i0, i1, i2 etc.
   */
  def za_hash_longs_with_struct(digestImpl: String, cols: Column*): Column =
    hashF(true, digestImpl, ZALongTupleHashFunctionFactory, cols: _*)

}
