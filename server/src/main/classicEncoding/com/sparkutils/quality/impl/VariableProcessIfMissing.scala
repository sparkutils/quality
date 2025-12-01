package com.sparkutils.quality.impl

import com.sparkutils.quality.RuleSuite
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.StructType

object VariableProcessIfMissing {

  protected[quality] def registerProcessIfAttributeMissingForAgnostic(registerFunction: (String, Seq[Expression] => Expression) => Unit): Unit = {

  }
}

trait VariableProcessIfMissing {

}
