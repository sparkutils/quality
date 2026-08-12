package com.sparkutils.quality.impl.rng

import org.apache.commons.rng.simple.RandomSource
import org.apache.spark.sql.{Column, ShimUtils}
import org.apache.spark.sql.functions.lit

trait RngFunctionImports {
  /**
   * Creates a random number generator using a given commons-rng source
   *
   * @param randomSource commons-rng random source
   * @param numBytes     the number of bytes to produce in the array, defaulting to 16
   * @param seed         the seed to use / mixin
   * @return a column with the appropriate rng defined
   */
  def rng_bytes(randomSource: RandomSource = RandomSource.XO_RO_SHI_RO_128_PP, numBytes: Int = 16, seed: Long = 0): Column =
     ShimUtils.callFunction("rng_bytes", lit(randomSource.name()), lit(seed), lit(numBytes))

  /**
   * Creates a uuid from byte arrays or two longs, use with the rng() function to generate random uuids.
   *
   * @param child the expression to produce a BinaryType
   */
  def rng_uuid(column: Column): Column =
    ShimUtils.callFunction("rng_uuid", column)
}
