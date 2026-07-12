package com.sparkutils.quality.impl.extension

import com.sparkutils.quality.impl.{HasOutput, Runner}
import com.sparkutils.testing.SparkVersions
import org.apache.spark.sql.catalyst.expressions.Expression

object ZeroCodeGenWrap {

  def wrap(runner: Runner): Expression = {
    val nr = runner.withZeroCode()
    ZeroCodeGen(nr, nr, on32 = SparkVersions.sparkVersion == "3.2")
  }
}
