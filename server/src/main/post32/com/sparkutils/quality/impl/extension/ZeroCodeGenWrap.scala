package com.sparkutils.quality.impl.extension

import com.sparkutils.quality.impl.Runner
import com.sparkutils.testing.SparkVersions
import org.apache.spark.sql.catalyst.expressions.{CodegenObjectFactoryMode, Expression}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.CODEGEN_FACTORY_MODE

object ZeroCodeGenWrap {

  def wrap(runner: Runner): Expression = {
    val fallbackMode = SQLConf.get.getConf(CODEGEN_FACTORY_MODE).toString

    if (fallbackMode == CodegenObjectFactoryMode.NO_CODEGEN.toString)
      runner  // when running in no_codegen the real children should be present otherwise SubExpressionEliminationRuntime cannot find subexprs
    else {
      // CodegenObjectFactoryMode.CODEGEN_ONLY as well as fallback
      val nr = runner.withZeroCode()
      ZeroCodeGen(nr, nr, on32 = SparkVersions.sparkVersion == "3.2")
    }
  }
}
