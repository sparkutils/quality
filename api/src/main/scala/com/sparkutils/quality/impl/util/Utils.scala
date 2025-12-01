package com.sparkutils.quality.impl.util

import com.sparkutils.quality._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StructType

object DebugTime extends Logging {

  def debugTime[T](what: String, log: (Long, String)=>Unit = (i, what) => {logDebug(s"----> ${i}ms for $what")} )( thunk: => T): T = {
    val start = System.currentTimeMillis
    try {
      thunk
    } finally {
      val stop = System.currentTimeMillis

      log(stop - start, what)
    }
  }

}

sealed trait LookupType {
  val name: String
}

case class MapLookupType(name: String) extends LookupType
case class BloomLookupType(name: String) extends LookupType

/**
 * Represents the results of lookups.  RuleRows will have empty expressions
 *
 * @param ruleSuite
 * @param ruleResults
 * @param lambdaResults it's not always possible to toString against an expression tree
 */
case class LookupResults(ruleSuite: RuleSuite, ruleResults: ExpressionLookupResults[RuleRow], lambdaResults: ExpressionLookupResults[Id])

case class ExpressionLookupResults[A](lookupConstants: Map[A, Set[LookupType]], lookupExpressions: Set[A])

case class ExpressionLookupResult(constants: Set[LookupType], hasExpressionLookups: Boolean)


object LookupIdFunctions {

  def namesFromSchema(schema: StructType): Set[String] = {

    def withParent(name: String, parent: String) =
      if (parent.isEmpty)
        name
      else
        parent + "." + name

    def accumulate(set: Set[String], schema: StructType, parent: String): Set[String] =
      schema.foldLeft(set) {
        (s, field) =>
          val name = withParent(field.name, parent)
          field.dataType match {
            case struct: StructType =>
              accumulate(s + name, struct, name)
            case _ => s + name
          }
      }

    accumulate(Set.empty, schema, "")
  }

}

object Comparison {

  /**
   * Forwards to compareTo, allows for compareTo[Type] syntax with internal casts
   * @param left
   * @param right
   * @tparam T
   * @return
   */
  def compareTo[T <: Comparable[T]](left: Any, right: Any): Int =
    if (left == null && right != null)
      -100
    else
      if (right == null && left != null)
        100
      else
        left.asInstanceOf[T].compareTo( right.asInstanceOf[T])

  /**
   * Forwards to compare, allows for compareToOrdering(ordering) syntax with internal casts
   * @param left
   * @param right
   * @tparam T
   * @return
   */
  def compareToOrdering[T](ordering: Ordering[T])(left: Any, right: Any): Int =
    if (left == null && right != null)
      -100
    else
      if (right == null && left != null)
        100
      else
        ordering.compare(left.asInstanceOf[T], right.asInstanceOf[T])

}

object Optional {
  def toOptional[T](option: Option[T]): java.util.Optional[T] =
    if (option.isEmpty)
      java.util.Optional.empty()
    else
      java.util.Optional.of(option.get)
}
