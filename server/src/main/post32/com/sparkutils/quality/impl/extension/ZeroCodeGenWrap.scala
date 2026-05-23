package com.sparkutils.quality.impl.extension

import com.sparkutils.quality.groupProcessorKey
import com.sparkutils.quality.impl.{Runner, Triggers}
import com.sparkutils.testing.SparkVersions
import org.apache.spark.sql.catalyst.expressions.Expression

object ZeroCodeGenWrap {

  def wrap(runner: Runner): Expression =
    if (runner.children.size > 800 || Triggers.getValue(groupProcessorKey, runner.extraConfig, "").nonEmpty) {
      val nr = runner.withZeroCode()
      ZeroCodeGen(nr, nr, on32 = SparkVersions.sparkVersion == "3.2")
    } else
      runner

}
