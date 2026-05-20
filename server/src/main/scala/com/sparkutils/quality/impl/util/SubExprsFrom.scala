package com.sparkutils.quality.impl.util

import org.apache.spark.sql.catalyst.expressions.codegen.ShimExprUtils
import org.apache.spark.sql.catalyst.expressions.{EquivalentExpressions, Expression}

object SubExprsFrom {
  def apply(expressions: Seq[Expression]) = {
    val eq = new EquivalentExpressions

    expressions.foreach(e => eq.addExprTree(e))
    val subs = ShimExprUtils.getAllEquivalentExprs(eq)

    ShimExprUtils.orderedByCount(subs, eq)
  }

}
