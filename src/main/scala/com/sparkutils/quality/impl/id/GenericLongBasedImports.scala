package com.sparkutils.quality.impl.id

import com.sparkutils.quality.impl.hash.{HashFunctionFactory, HashFunctionsExpression, MessageDigestFactory}
import com.sparkutils.quality.impl.rng.RandLongsWithJump
import org.apache.commons.rng.simple.RandomSource
import org.apache.spark.sql.{Column, ShimUtils}
import org.apache.spark.sql.ShimUtils.{callFunction, column, expression}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.shim.hash.DigestFactory

trait GenericLongBasedImports {
  /**
   * Creates a default randomRNG based on RandomSource.XO_RO_SHI_RO_128_PP
   */
  def rngID(prefix: String): Column =
    callFunction("rng_id", lit(prefix))

  /**
   * Creates a randomRNG ID based on randomSource with a given seed
   */
  def rng_id(prefix: String, randomSource: RandomSource, seed: Long = 0L): Column =
    callFunction("rng_id", lit(prefix), lit(randomSource.name()), lit(seed))

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
    callFunction("provided_id", lit(prefix), child)

  /**
   * Murmur3 hash
   * @param prefix
   * @param children
   * @return
   */
  def murmur3ID(prefix: String, children: Seq[Column]): Column =
    callFunction("murmur3_id", (Seq(lit(prefix)) ++ children ) :_*)

  def murmur3ID(prefix: String, child1: Column, restOfchildren: Column*): Column =
    murmur3ID(prefix, child1 +: restOfchildren)

}
