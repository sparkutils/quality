package com.sparkutils.quality.impl

import org.apache.spark.sql.catalyst.expressions.Expression

object VariableProcessIfMissingImpl {

  protected[quality] def registerProcessIfAttributeMissingForAgnostic(registerFunction: (String, Seq[Expression] => Expression) => Unit): Unit = {

  }
}
