package com.sparkutils.quality.impl.extension

import com.sparkutils.quality.PredicateHelperPlus
import com.sparkutils.quality.impl.UUIDToLongsExpression
import com.sparkutils.quality.impl.id.{AsBase64Fields, AsBase64Struct, IDFromBase64, IDToRawIDDataType, SizeOfIDString}
import com.sparkutils.quality.impl.longPair.AsUUID
import org.apache.spark.sql.catalyst.expressions.{And, BinaryComparison, CreateNamedStruct, CreateStruct, EqualNullSafe, EqualTo, Equality, Expression, GetStructField, GreaterThan, GreaterThanOrEqual, If, In, LessThan, LessThanOrEqual, Literal, Or}
import org.apache.spark.sql.catalyst.plans.logical.{ConstraintHelper, Filter, Join, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.StringType

/**
 * Replaces any attributes aliased by an expression which is then used in select filters / constraints wth
 * constituent predicates that can be pushed down.  A simple example is:
 * {{{
 *     select as_uuid(lower, higher) as context
 *     where context = 'string'
 * }}}
 * this would be replaced with:
 * {{{
 *     select as_uuid(lower, higher) as context
 *     where (lower = longPairFromUUID('string').lower and higher = longPairFromUUID('string').higher)
 * }}}
 * in the case of the string being a constant, constant folding should replace both of these with simple lookups that are easy to push down.
 *
 * *NB.* Pushdown predicates based on constant folding only work via the SparkExtension approach, the use of experimental.extraOptimizations does provide this as it happens too late in the plans
 *
 * Only In and BinaryComparison's are supported currently
 */
abstract class AsymmetricFilterExpressions extends Rule[LogicalPlan]
  with PredicateHelperPlus with ConstraintHelper {

  /**
   * Should the matchOnExpression be defined in a filter provide the rewrite.  It's possible for joins that both comparedTo AND generating are the
   * same expression type, this should be explicitly checked for.
   *
   * Implementations must return None for any expression they cannot handle, not doing so can lead to MatchErrors.
   *
   * @param generating the underlying expression which is generating either an Alias or the direct expression itself
   * @param comparedTo either an Expression for BinaryComparison where it can be used transparently, irrespective of usage (NamedExpression, Expression etc.), or Seq[Expression] for In's.
   *                   Where In's are used and the re-write involves structures you should aim to repeat the constituent parts as separate In's in order to get predicate push down
   * @param filter the Expression used in the filter e.g. EqualTo, In, GreaterThan etc.
   * @return a rewritten expression tree, logically the opposite of the generating expression or None where generating does not match
   */
  def reWriteExpression(generating: Expression, comparedTo: Object, filter: Expression): Option[Expression]

  // attempt to find / replace any part of the filter tree
  def transform(expr: Expression, topPlan: LogicalPlan): (Expression, Boolean) = {
    var found = false
    (expr.transform {
      case i@In(expr, list) =>
        findRootExpression(expr, topPlan).map { lhs =>
          reWriteExpression(lhs, list, i).map { e => found = true; e }.getOrElse(i)
        }.getOrElse(i)
      case e@BinaryComparison(elhs, erhs) =>
        findRootExpression(elhs, topPlan).flatMap{ a =>
          val rhs = findRootExpression(erhs, topPlan)
          rhs.map( b => (a, b) )
        }.map {
          case (lhs, rhs) =>
            // flip the sides as needed
            reWriteExpression(lhs, rhs, e).orElse(
              reWriteExpression(rhs, lhs, e)
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

object AsymmetricFilterExpressions {
  /**
   * TODO When 2.4 is dropped move this to withChildren
   */
  def create(expressionTemplate: Expression, left: Expression, right: Expression): Expression = expressionTemplate match {
    case _:EqualTo => EqualTo(left, right)
    case _:EqualNullSafe => EqualNullSafe(left, right)
    case _:LessThan => LessThan(left, right)
    case _:LessThanOrEqual => LessThanOrEqual(left, right)
    case _:GreaterThan => GreaterThan(left, right)
    case _:GreaterThanOrEqual => GreaterThanOrEqual(left, right)
  }
}

/**
 * Optimises as_uuid usage for filters on the generated value, swapping out for long lookups
 */
object AsUUIDFilter extends AsymmetricFilterExpressions {

  override def reWriteExpression(generating: Expression, comparedTo: Object,
                                 filter: Expression): Option[Expression] = {
    val create = AsymmetricFilterExpressions.create(filter,_: Expression,_: Expression)
    (generating, comparedTo, filter) match {
      // join case (possibly a filter)
      case (AsUUID(alower, ahigher), AsUUID(blower, bhigher), Equality(_,_) ) =>
        Some(
          And( create(alower, blower), create(ahigher, bhigher))
        )
      // filter case's
      case (AsUUID(lower, higher), c: Expression, Equality(_,_)) if c.dataType == StringType =>
        Some(
          And( create( GetStructField( UUIDToLongsExpression(c), 0) , lower), create( GetStructField( UUIDToLongsExpression(c), 1 ), higher) )
        )
      // In verifies the rest of the seq are the same type
      case (a@AsUUID(lower, higher), l: Seq[Expression @unchecked], _) if l.headOption.exists(_.dataType == StringType) =>
        def struct(lower: Expression, higher: Expression): Expression =
          CreateNamedStruct(Seq(Literal("lower"), lower,
            Literal("higher"), higher))

        def structg(expr: Expression): Expression =
          struct( GetStructField(UUIDToLongsExpression(expr), 0), GetStructField(UUIDToLongsExpression(expr), 1))

        Some(
          And( In(struct(lower, higher), l.map(structg _)),
            And( In(lower, l.map(t => GetStructField(UUIDToLongsExpression(t), 0))),
              In(higher, l.map(t => GetStructField(UUIDToLongsExpression(t), 1)))
            ))
        )
      // join + filter
      case (AsUUID(alower, ahigher), AsUUID(blower, bhigher), _: LessThan | _: LessThanOrEqual | _: GreaterThan | _: GreaterThanOrEqual) =>
        Some(
          Or(And(EqualTo(ahigher, bhigher), create(alower, blower)), create(ahigher, bhigher))
        )
      // filter case's
      case (AsUUID(lower, higher), c: Expression, _: LessThan | _: LessThanOrEqual | _: GreaterThan | _: GreaterThanOrEqual) if c.dataType == StringType =>
        val blower = GetStructField(UUIDToLongsExpression(c), 0)
        val bhigher = GetStructField(UUIDToLongsExpression(c), 1)

        Some(
          Or(And(EqualTo(higher, bhigher), create(lower, blower)), create(higher, bhigher))
        )
      case _ => None
    }
  }

}


/**
 * Optimises id_base64 usage for filters on the generated base64 value, swapping out for int / long lookups
 */
object IDBase64Filter extends AsymmetricFilterExpressions {

  def andFields[T](size: Int, leftF: Int => Expression, rightF: Int => T, create: (Expression, T) => Expression ): Expression = {
    val useSize = size + 1
    def andFields(i: Int): Expression =
      if (i < useSize)
        And(create(leftF(i), rightF(i)), andFields(i + 1))
      else
        Literal.TrueLiteral

    andFields(0)
  }

  def orAndFields(size: Int, leftF: Int => Expression, rightF: Int => Expression, create: (Expression, Expression) => Expression ): Expression = {

    def orAndFields(useSize: Int): Expression =
      if (useSize > 1)
        Or(And(andFields(useSize - 2, leftF, rightF, EqualTo(_: Expression, _: Expression)), create(leftF(useSize-1), rightF(useSize-1))),
          orAndFields(useSize - 1) )
      else
        create(leftF(0), rightF(0))

    orAndFields(size + 1)
  }

  def wrapSizeCheck(size: Int, c: Expression, andFields: Expression): Expression =
    If(EqualTo(Literal(size), SizeOfIDString(c)), // don't compare if the sizes aren't the same
      andFields,
      Literal.FalseLiteral
    )

  def inComp(size: Int, left: Expression, l: Seq[Expression]) =
    Some(
      And( In( left, l.map(IDFromBase64( _, size))),
        andFields(size, GetStructField(left, _), i => l.map(t => GetStructField(IDFromBase64(t, size), i)), In(_: Expression,_:Seq[Expression]))
      )
    )

  override def reWriteExpression(generating: Expression, comparedTo: Object,
                                 filter: Expression): Option[Expression] = {
    val create = AsymmetricFilterExpressions.create(filter,_: Expression,_: Expression)
    (generating, comparedTo, filter) match {
      // join case (possibly a filter)
      case (a@ AsBase64Struct(left), b@ AsBase64Struct(right), Equality(_,_) ) =>
        if (a.size != b.size) None
        else
          Some(
            andFields(a.size, GetStructField(left, _), GetStructField(right, _), create)
          )
      case (a@ AsBase64Fields(leftFields), b@ AsBase64Struct(right), Equality(_,_) ) =>
        if (a.size != b.size) None
        else
          Some(
            andFields(a.size, leftFields(_), GetStructField(right, _), create)
          )
      case (a@ AsBase64Fields(leftFields), b@ AsBase64Fields(rightFields), Equality(_,_) ) =>
        if (a.size != b.size) None
        else
          Some(
            andFields(a.size, leftFields(_), rightFields(_), create)
          )
      // filter case's
      case (a @ AsBase64Fields(leftFields), c: Expression, Equality(_,_)) if c.dataType == StringType =>
        val id = IDFromBase64(c, a.size)
        Some(
          wrapSizeCheck(a.size, c,
            andFields(a.size, leftFields(_), GetStructField(id, _), create))
        )
      case (a @ AsBase64Struct(left), c: Expression, Equality(_,_)) if c.dataType == StringType =>
        val id = IDFromBase64(c, a.size)
        Some(
          wrapSizeCheck(a.size, c,
            andFields(a.size, GetStructField(left, _), GetStructField(id, _), create))
        )

      // In verifies the rest of the seq are the same type
      case (a@AsBase64Struct(left), l: Seq[Expression @unchecked], _) if l.headOption.exists(_.dataType == StringType) =>
        inComp(a.size, left, l)

      case (a@AsBase64Fields(left), l: Seq[Expression @unchecked], _) if l.headOption.exists(_.dataType == StringType) =>
        inComp(a.size, CreateStruct(left), l)

      // join + filter
      case (a@AsBase64Fields(left), b@AsBase64Fields(right), _: LessThan | _: LessThanOrEqual | _: GreaterThan | _: GreaterThanOrEqual) =>
        if (a.size != b.size)
          None
        else
          Some(
            orAndFields(a.size, left(_), right(_), create)
          )

      case (a@AsBase64Fields(left), b@AsBase64Struct(right), _: LessThan | _: LessThanOrEqual | _: GreaterThan | _: GreaterThanOrEqual) =>
        if (a.size != b.size)
          None
        else
          Some(
            orAndFields(a.size, left(_), GetStructField(right, _), create)
          )

      case (a@AsBase64Struct(left), b@AsBase64Struct(right), _: LessThan | _: LessThanOrEqual | _: GreaterThan | _: GreaterThanOrEqual) =>
        if (a.size != b.size)
          None
        else
          Some(
            orAndFields(a.size, GetStructField(left, _), GetStructField(right, _), create)
          )

      // filter case's
      case (a@AsBase64Struct(left), c: Expression, _: LessThan | _: LessThanOrEqual | _: GreaterThan | _: GreaterThanOrEqual) if c.dataType == StringType =>
        val id = IDFromBase64(c, a.size)
        Some(
          wrapSizeCheck(a.size, c,
            orAndFields(a.size, GetStructField(left, _), GetStructField(id, _), create))
        )

      case (a@AsBase64Fields(left), c: Expression, _: LessThan | _: LessThanOrEqual | _: GreaterThan | _: GreaterThanOrEqual) if c.dataType == StringType =>
        val id = IDFromBase64(c, a.size)
        Some(
          wrapSizeCheck(a.size, c,
            orAndFields(a.size, left(_), GetStructField(id, _), create))
        )

      case _ => None
    }
  }

}