package com.sparkutils.quality.impl.id

import com.sparkutils.quality.impl.hash.{HashFunctionFactory, HashFunctionsExpression, MessageDigestFactory}
import com.sparkutils.quality.impl.rng.RandLongsWithJump
import org.apache.commons.rng.simple.RandomSource
import org.apache.spark.sql.Column
import org.apache.spark.sql.ShimUtils.{column, expression}
import org.apache.spark.sql.shim.hash.DigestFactory

trait GenericLongBasedImports {
  /**
   * Creates a default randomRNG based on RandomSource.XO_RO_SHI_RO_128_PP
   */
  def rngID(prefix: String): Column =
    column(GenericLongBasedIDExpression(model.RandomID,
      RandLongsWithJump(0L, RandomSource.XO_RO_SHI_RO_128_PP), prefix))

  /**
   * Creates a randomRNG ID based on randomSource with a given seed
   */
  def rng_id(prefix: String, randomSource: RandomSource, seed: Long = 0L): Column =
    column( GenericLongBasedIDExpression (model.RandomID,
      RandLongsWithJump(seed, randomSource), prefix) )

  /**
   * Creates a hash based ID based on a 128 bit MD5 by default
   * @param prefix
   * @return
   */
  def fieldBasedID(prefix: String, children: Seq[Column], digestImpl: String = "MD5", digestFactory: String => DigestFactory = MessageDigestFactory): Column =
    column(GenericLongBasedIDExpression(model.FieldBasedID,
      HashFunctionsExpression(children.map(expression(_)), digestImpl, true, digestFactory(digestImpl)), prefix))

  // NB field_based_id is in HashRelatedFunctionImports, same impl and interface but fits the sql name

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
    fieldBasedID(prefix, children, digestImpl)

  /**
   * Creates a hash based ID based on an upstream compatible long generator, in line with sql functions please migrate to provided_id
   * @param prefix
   * @return
   */
  @deprecated(since = "0.1.0", message = "migrate to provided_id")
  def providedID(prefix: String, child: Column): Column =
    provided_id(prefix, child)

  /**
   * Creates a hash based ID based on an upstream compatible long generator
   *
   * @param prefix
   * @return
   */
  def provided_id(prefix: String, child: Column): Column =
    column(GenericLongBasedIDExpression(model.ProvidedID, expression(child), prefix))

  /**
   * Murmur3 hash
   * @param prefix
   * @param children
   * @param digestImpl - only Murmur3 currently supported
   * @return
   */
  def hashID(prefix: String, children: Seq[Column], digestImpl: String = "IGNORED"): Column =
    column(GenericLongBasedIDExpression(model.FieldBasedID,
      HashFunctionsExpression(children.map(expression(_)), digestImpl, true, HashFunctionFactory("IGNORED")), prefix))

  def hashID(prefix: String, digestImpl: String, children: Column*): Column = hashID(prefix, children, digestImpl)

  /**
   * Murmur3 hash
   * @param prefix
   * @param children
   * @return
   */
  def murmur3ID(prefix: String, children: Seq[Column]): Column = hashID(prefix, children, "M3_128")
  def murmur3ID(prefix: String, child1: Column, restOfchildren: Column*): Column = hashID(prefix, child1 +: restOfchildren, "M3_128")

}
