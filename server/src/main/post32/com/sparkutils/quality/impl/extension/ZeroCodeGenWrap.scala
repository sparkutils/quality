package com.sparkutils.quality.impl.extension

import com.sparkutils.quality.impl.{HasOutput, Runner}
import com.sparkutils.testing.SparkVersions
import org.apache.spark.sql.catalyst.expressions.{CodegenObjectFactoryMode, Expression}
import org.apache.spark.sql.internal.SQLConf

object ZeroCodeGenWrap {

  def wrap(runner: Runner): Expression = {
    val fallbackMode = SQLConf.get.codegenFactoryMode

    fallbackMode match {
      case CodegenObjectFactoryMode.NO_CODEGEN =>
        runner  // when running in no_codegen the real children should be present
      case _  => // CodegenObjectFactoryMode.CODEGEN_ONLY as well as fallback
        val nr = runner.withZeroCode()
        ZeroCodeGen(nr, nr, on32 = SparkVersions.sparkVersion == "3.2")
    }
  }
}
