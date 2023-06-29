package com.sparkutils.quality

import org.apache.spark.sql.DataFrame

/**
 * Simple marker instead of sys.error
 * @param msg
 * @param cause
 */
case class QualityException(msg: String, cause: Exception = null) extends RuntimeException(msg, cause)

object QualityException {
  def qualityException(msg: String, cause: Exception = null) = throw QualityException(msg, cause)
}

/**
 * Simple interface to load DataFrames used by map/bloom and view loading
 */
trait DataFrameLoader {
  def load(token: String): DataFrame
}
