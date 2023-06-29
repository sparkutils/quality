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
    case _ => Some(expr) // 2.4 works for this, at least in the tests
  }

}
