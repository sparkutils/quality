package com.sparkutils.quality.impl.util

import org.apache.spark.sql.catalyst.expressions.codegen.QualityExprUtils
import org.apache.spark.sql.catalyst.expressions.{EquivalentExpressions, Expression}

object SubExprsFrom {
  def apply(expressions: Seq[Expression]) = {
    val eq = new EquivalentExpressions

    expressions.foreach(e => eq.addExprTree(e))
    val subs = QualityExprUtils.getAllEquivalentExprs(eq)

    QualityExprUtils.orderedByCount(subs, eq)
  }

}
