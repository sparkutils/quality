package com.sparkutils.quality.impl.util

import org.apache.spark.sql.catalyst.expressions.Expression

object SubExprs {
  def apply(expressions: Seq[Expression]): Expression => Option[(Int, Expression)] = {
    ???
  }
}
