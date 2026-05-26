package com.sparkutils.quality.impl.util

import org.apache.spark.sql.catalyst.expressions.{EquivalentExpressions, Expression}

object SubExprsFrom {
  def apply(expressions: Seq[Expression]): Expression => Option[(Int, Expression)] = {
    val eq = new EquivalentExpressions

    expressions.foreach(e => eq.addExprTree(e))
    eq.getExprState(_: Expression).map(e => (e.useCount, e.expr))
  }
}
