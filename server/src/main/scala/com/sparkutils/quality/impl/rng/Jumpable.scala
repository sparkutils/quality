package com.sparkutils.quality.impl.rng

import org.apache.commons.rng.JumpableUniformRandomProvider

/**
 * Supports jumping to enable multiple threads to run in isolation
 */ //TODO - still need to validate the usage in practice on a big cluster for collisions
trait Jumpable extends RngImpl {
  type ThisType <: Jumpable

  type Provider <: JumpableUniformRandomProvider

  /**
   * Jumps via RNG jump 2^^64 worth of iterations, which should be enough for a given dataset processing
   */
  override def branch: Unit = {
    rng = rng.jump().asInstanceOf[Provider]
  }

  /**
   * Prefers to branch (via jump) rather than create a new and possibly colliding rng
   *
   * @param seed used only when the rng has not yet been created, otherwise branch will be used
   */
  override def reSeedOrBranch(seed: Long) {
    if (isNull)
      reSeed(seed)
    else
      branch
  }
}