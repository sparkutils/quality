package com.sparkutils.quality.impl.util

import org.apache.spark.sql.catalyst.expressions.Expression

object SubExprsFrom {
  def apply(expressions: Seq[Expression]): Expression => Option[(Int, Expression)] = {
    ???
  }
}
