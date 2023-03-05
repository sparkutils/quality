package com.sparkutils.quality.impl.rng

import org.apache.commons.rng.UniformRandomProvider
import org.apache.commons.rng.simple.RandomSource

/**
 * Provides a default byte array support - other implementations can mix-in as needed
 */
trait RngImpl extends Product {
  type ThisType <: RngImpl

  type Provider <: UniformRandomProvider

  @transient var rng: Provider = _

  /**
   * Allows providing a seed for use with reSeed
   * @return
   */ // TODO figure out if we should just do byte array and then in reSeed xor it with the seed bits
  def definedSeed: Long

  /**
   * Creates via the RNG default random seed, other implementations may jump or split
   */
  def branch: Unit = reSeed(0)

  /**
   * The implementation of the RNG, use the RandomSource enums.
   * @return
   */
  def source: RandomSource

  /**
   * Using this seed create the RNG, most support Long but it may be truncated.
   *
   * The seed will be partitionIndex in addition to definedSeed
   *
   * @param seed if 0 is used then definedSeed is used in addition to a default random long
   */
  def reSeed(seed: Long) {
    val defSeed = source.createSeed()

    rng = RandomSource.create(source, defSeed).asInstanceOf[Provider]
  }

  /**
   * Allow branching logic where possible but fallback to reseed as needed
   *
   * By default delegates to reSeed
   *
   * @param seed
   */
  def reSeedOrBranch(seed: Long): Unit = reSeed(seed)

  def isNull: Boolean = rng eq null

  /**
   * Number of bytes to fill for nextBytes
   */
  def numBytes: Int

  def nextBytes(): Array[Byte] = {
    val res =  Array.ofDim[Byte](numBytes)
    rng.nextBytes(res)
    res
  }

  def nextLong(): Long = {
    rng.nextLong()
  }

  /**
   * Create a copy of this type, must be implemented in the actual expression and must capture / implement the definedSeed
   * @return
   */
  def freshCopy(): ThisType
}
