package com.sparkutils.quality.impl.extension

import com.sparkutils.quality.PredicateHelperPlus
import com.sparkutils.quality.impl.UUIDToLongsExpression
import com.sparkutils.quality.impl.longPair.{AsUUID, LongPair}
import org.apache.spark.sql.catalyst.expressions.{And, BoundReference, EqualTo, Expression, ExpressionSet, GetStructField, Literal, PredicateHelper}
import org.apache.spark.sql.catalyst.optimizer.InferFiltersFromConstraints.{constructIsNotNullConstraints, inferAdditionalConstraints, splitConjunctivePredicates}
import org.apache.spark.sql.catalyst.plans.{InnerLike, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.{ConstraintHelper, Filter, Join, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf

/**
 * Replaces any attributes aliased by an expression which is then used in select filters / constraints wth
 * constituent predicates that can be pushed down.  A simple example is:
 * ```
 *   select as_uuid(lower, higher) as context
 *   where context = 'string'
 * ```
 * this would be replaced with:
 * ```
 *   select as_uuid(lower, higher) as context
 *   where (lower = longPairFromUUID('string').lower and higher = longPairFromUUID('string').higher)
 * ```
 * in the case of the string being a constant, constant folding should replace both of these with simple lookups that are easy to push down.
 *
 * Only EqualTo is supported currently and expressions not used in select (i.e. as_uuid(lower ,higher) = 'uuid-string'
 */
abstract class AsymmetricFilterExpressions extends Rule[LogicalPlan]
  with PredicateHelperPlus with ConstraintHelper {

  /**
   * Should the matchOnExpression be defined in a filter provide the rewrite.  It's possible for joins that both comparedTo AND generating are the
   * same expression type, this should be explicitly checked for.
   *
   * @param comparedTo can be used transparently, irrespective of usage (NamedExpression, Expression etc.)
   * @param generating the underlying expression which is generating either an Alias or the direct expression itself
   * @return a rewritten expression tree, logically the opposite of the generating expression or None where generating does not match
   */
  def reWriteExpression(generating: Expression, comparedTo: Expression): Option[Expression]

  // attempt to find / replace any part of the filter tree
  def transform(expr: Expression, topPlan: LogicalPlan): (Expression, Boolean) = {
    var found = false
    (expr.transform {
      case e@EqualTo(elhs, erhs) =>
        findRootExpression(elhs, topPlan).flatMap{ a =>
          val rhs = findRootExpression(erhs, topPlan)
          rhs.map( b => (a, b) )
        }.map {
          case (lhs, rhs) =>
            // flip the sides as needed
            reWriteExpression(lhs, rhs).orElse(
              reWriteExpression(rhs, lhs)
            ).map { e => found = true; e }.getOrElse(e)
        }.getOrElse(e)
    }, found)
  }

  class FilterTransformer(topPlan: LogicalPlan) {
    def unapply(plan: LogicalPlan): Option[(Expression, LogicalPlan)] =
      plan match {
        case Filter(expr, child) =>
          val (r, found) = transform(expr, topPlan)
          if (found)
            Some((r, child))
          else
            None
        case _ => None
      }
  }

  class JoinTransformer(topPlan: LogicalPlan) {
    def unapply(plan: LogicalPlan): Option[Join] =
      plan match {
        case j: Join =>
          // attempt re-write for join
          j.condition.flatMap { condition =>

            val (r, found) = transform(condition, topPlan)

            if (found)
              Some(j.copy(condition = Some(r)))
            else
              None
          }
        case _ => None
      }
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    val FilterT = new FilterTransformer(plan)
    val JoinT = new JoinTransformer(plan)
    plan transform {
      case FilterT(resulting, child) =>
        Filter(resulting, child)
      case JoinT(join) =>
        join
    }
  }

}

/**
 * Optimises as_uuid usage for filters on the generated value, swapping out for long lookups
 */
object AsUUIDFilter extends AsymmetricFilterExpressions {

  override def reWriteExpression(generating: Expression, comparedTo: Expression): Option[Expression] = (generating, comparedTo) match {
    // join case
    case (AsUUID(alower, ahigher), AsUUID(blower, bhigher)) =>
      Some(
        And(EqualTo(alower, blower), EqualTo(ahigher, bhigher))
      )
    // filter case
    case (AsUUID(lower, higher), _) =>
      Some(
        And( EqualTo( GetStructField( UUIDToLongsExpression(comparedTo), 0) , lower), EqualTo( GetStructField( UUIDToLongsExpression(comparedTo), 1 ), higher) )
      )
    case _ => None
  }

}