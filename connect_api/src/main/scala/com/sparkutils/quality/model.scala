package com.sparkutils.quality

import org.apache.spark.sql.DataFrame

/**
 * Simple marker instead of sys.error
 * @param msg
 * @param cause
 */
@SerialVersionUID(1L)
case class QualityException(msg: String, cause: Throwable = null) extends RuntimeException(msg, cause)

object QualityException {
  def qualityException(msg: String, cause: Throwable = null): Nothing = throw QualityException(msg, cause)
}

/**
 * Simple interface to load DataFrames used by map/bloom and view loading
 */
trait DataFrameLoader {
  def load(token: String): DataFrame
}
