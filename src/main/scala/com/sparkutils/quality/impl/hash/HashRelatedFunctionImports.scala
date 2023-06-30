package com.sparkutils.quality.impl.hash

import com.sparkutils.quality.impl.id.{GenericLongBasedIDExpression, model}
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.qualityFunctions.DigestFactory

trait HashRelatedFunctionImports {

  protected def digest_to_longsF(asStruct: Boolean, digestImpl: String, cols: Column*): Column =
    new Column(HashFunctionsExpression(cols.map(_.expr), digestImpl, asStruct, MessageDigestFactory(digestImpl)))

  /**
   * Converts columns into a digest via the MessageDigest digestImpl
   *
   * @param digestImpl
   * @param cols
   * @return array of long
   */
  def digest_to_longs(digestImpl: String, cols: Column*): Column =
    digest_to_longsF(false, digestImpl, cols: _*)

  /**
   * Converts columns into a digest via the MessageDigest digestImpl
   *
   * @param digestImpl
   * @param cols
   * @return struct with fields i0, i1, i2 etc.
   */
  def digest_to_longs_struct(digestImpl: String, cols: Column*): Column =
    digest_to_longsF(true, digestImpl, cols: _*)

  protected def fieldBasedIDF(prefix: String, digestImpl: String, factory: String => DigestFactory, cols: Column*): Column =
    new Column(GenericLongBasedIDExpression(model.FieldBasedID,
      HashFunctionsExpression(cols.map(_.expr), digestImpl, true, factory(digestImpl)), prefix))

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

}
