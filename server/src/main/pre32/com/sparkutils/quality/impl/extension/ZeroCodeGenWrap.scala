package com.sparkutils.quality.impl.extension

import com.sparkutils.quality.impl.Runner
import org.apache.spark.sql.catalyst.expressions.Expression

// pre 3.2 doesn't use withNewChildren copy constructors so it cannot ever swap the impl.
object ZeroCodeGenWrap {

  def wrap(runner: Runner): Expression =
    runner

}
