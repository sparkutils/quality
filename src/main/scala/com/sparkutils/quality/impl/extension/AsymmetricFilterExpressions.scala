package com.sparkutils.quality.impl.extension

import com.sparkutils.quality.impl.UUIDToLongsExpression
import com.sparkutils.quality.impl.longPair.{AsUUID, LongPair}
import org.apache.spark.sql.catalyst.expressions.{And, BoundReference, EqualTo, Expression, ExpressionSet, GetStructField, PredicateHelper}
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
  with PredicateHelper with ConstraintHelper {

  /**
   * Should the matchOnExpression be defined in a filter provide the rewrite.
   *
   * @param comparedTo can be used transparently, irrespective of usage (NamedExpression, Expression etc.)
   * @param generating the underlying expression which is generating either an Alias or the direct expression itself
   * @return a rewritten expression tree, logically the opposite of the generating expression or None where generating does not match
   */
  def reWriteExpression(generating: Expression, comparedTo: Expression): Option[Expression]

  object Transformer {
    def unapply(plan: LogicalPlan): Option[(Expression, LogicalPlan)] =
      plan match {
        case Filter(EqualTo(lhs, rhs), child) =>
          reWriteExpression(lhs, rhs).orElse(
            reWriteExpression(rhs, lhs)
          ).map((_,child))
        case _ => None
      }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Transformer(resulting, child) =>
      Filter(resulting, child)
    /*case filter @ Filter(condition, child) =>
      println(s"<--------  did actually get here with $filter")
      val newFilters = filter.constraints --
        (child.constraints ++ splitConjunctivePredicates(condition))
      if (newFilters.nonEmpty) {
        Filter(And(newFilters.reduce(And), condition), child)
      } else {
        filter
      }*/
  }

}

/**
 * Optimises as_uuid usage for filters on the generated value, swapping out for long lookups
 */
object AsUUIDFilter extends AsymmetricFilterExpressions {

  override def reWriteExpression(generating: Expression, comparedTo: Expression): Option[Expression] = generating match {
    case a@AsUUID(lower, higher) =>
      Some(
        And( EqualTo( GetStructField( UUIDToLongsExpression(comparedTo), 0) , lower), EqualTo( GetStructField( UUIDToLongsExpression(comparedTo), 1 ), higher) )
      )
    case _ => None
  }
}