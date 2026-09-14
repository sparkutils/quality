package com.sparkutils.quality.impl.util

import java.util.concurrent.atomic.AtomicInteger

trait GeneratedUniqueName {

  private val nameCounter = new AtomicInteger(0)

  protected val GENERATED_NAME_PREFIX: String

  // only for the current session, so regardless of on driver with static or connect client this works
  protected[quality] def uniqueName(): String = GENERATED_NAME_PREFIX + nameCounter.incrementAndGet()

}
