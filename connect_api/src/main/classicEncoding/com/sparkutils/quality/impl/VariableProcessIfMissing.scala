package com.sparkutils.quality.impl

import org.apache.spark.sql.catalyst.expressions.Expression

object VariableProcessIfMissingFunctions {

  protected[quality] def registerProcessIfAttributeMissingForAgnostic(registerFunction: (String, Seq[Expression] => Expression) => Unit): Unit = {

  }
}

trait VariableProcessIfMissing {

}
