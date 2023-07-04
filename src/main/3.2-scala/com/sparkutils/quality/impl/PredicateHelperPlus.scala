package com.sparkutils.quality.impl

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Wraps PredicateHelper, 2.4 doesn't have the findExpression function and it's needed for joins on 3.3
 */
trait PredicateHelperPlus extends PredicateHelper {

  /**
   * Spark 3.3.0 (possibly lower) returns None for a literal, which makes sense, 3.1.3 however returns the literal
   */
  def findRootExpression(expr: Expression, topPlan: LogicalPlan): Option[Expression] = expr match {
    case l: Literal => Some(l)
    case _ =>
      /* #29 thanks to Sandeep @ Databricks for the find that it was triggered by limit, so we have to dive
         possibly a plan which doesn't have the projection, it does not dive down itself */
      val res = findExpressionAndTrackLineageDown(expr, topPlan).map(_._1)
      res.orElse {
        topPlan.children.flatMap{
          child =>
            findRootExpression(expr, child)
        }.headOption
      }
  }

}
