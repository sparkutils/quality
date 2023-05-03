package com.sparkutils.quality.impl.extension

import com.sparkutils.quality.PredicateHelperPlus
import com.sparkutils.quality.impl.UUIDToLongsExpression
import com.sparkutils.quality.impl.longPair.{AsUUID, LongPair}
import org.apache.spark.sql.catalyst.expressions.{And, CreateNamedStruct, CreateStruct, EqualTo, Expression, GetStructField, In, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{ConstraintHelper, Filter, Join, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.StringType

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
   * @param generating the underlying expression which is generating either an Alias or the direct expression itself
   * @param comparedTo either an Expression for BinaryComparison where it can be used transparently, irrespective of usage (NamedExpression, Expression etc.), or Seq[Expression] for In's.
   *                   Where In's are used and the re-write involves structures you should aim to repeat the constituent parts as separate In's in order to get predicate push down
   * @return a rewritten expression tree, logically the opposite of the generating expression or None where generating does not match
   */
  def reWriteExpression(generating: Expression, comparedTo: Object): Option[Expression]

  // attempt to find / replace any part of the filter tree
  def transform(expr: Expression, topPlan: LogicalPlan): (Expression, Boolean) = {
    var found = false
    (expr.transform {
      case i@In(expr, list) =>
        findRootExpression(expr, topPlan).map { lhs =>
          reWriteExpression(lhs, list).map { e => found = true; e }.getOrElse(i)
        }.getOrElse(i)
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

  override def reWriteExpression(generating: Expression, comparedTo: Object): Option[Expression] = (generating, comparedTo) match {
    // join case (possibly a filter)
    case (AsUUID(alower, ahigher), AsUUID(blower, bhigher)) =>
      Some(
        And(EqualTo(alower, blower), EqualTo(ahigher, bhigher))
      )
    // filter case's
    case (AsUUID(lower, higher), c: Expression) if c.dataType == StringType =>
      Some(
        And( EqualTo( GetStructField( UUIDToLongsExpression(c), 0) , lower), EqualTo( GetStructField( UUIDToLongsExpression(c), 1 ), higher) )
      )
    // In verifies the rest of the seq are the same type
    case (a@AsUUID(lower, higher), l: Seq[Expression]) if l.headOption.exists(_.dataType == StringType) =>
      def struct(lower: Expression, higher: Expression): Expression =
        CreateNamedStruct(Seq(Literal("lower"), lower,
          Literal("higher"), higher))

      def structg(expr: Expression): Expression =
        struct( GetStructField(UUIDToLongsExpression(expr), 0), GetStructField(UUIDToLongsExpression(expr), 1))

      Some(
        And( In(struct(lower, higher), l.map(structg _)),
          And( In(lower, l.map(t => GetStructField(UUIDToLongsExpression(t), 0))),
            In(higher, l.map(t => GetStructField(UUIDToLongsExpression(t), 1))),
          ))
      )
    case _ => None
  }

}