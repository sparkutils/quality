package com.sparkutils.quality.impl

import org.apache.spark.sql.catalyst.expressions.Unevaluable

trait ThreeOnlyNonFoldable extends Unevaluable {

  // needed for Spark3, runs constant folder and Unevaluable on Spark 3 doesn't declare false
  override val foldable: Boolean = false

}
