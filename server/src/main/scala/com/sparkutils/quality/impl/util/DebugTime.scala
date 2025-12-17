package com.sparkutils.quality.impl.util

import org.apache.spark.internal.Logging

object DebugTime extends Logging {

  def debugTime[T](what: String, log: (Long, String) => Unit = (i, what) => {
    logDebug(s"----> ${i}ms for $what")
  })(thunk: => T): T = {
    val start = System.currentTimeMillis
    try {
      thunk
    } finally {
      val stop = System.currentTimeMillis

      log(stop - start, what)
    }
  }

}
