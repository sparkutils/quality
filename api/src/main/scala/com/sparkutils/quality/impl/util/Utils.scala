package com.sparkutils.quality.impl.util

import com.sparkutils.quality._
import org.apache.spark.sql.types.StructType

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

object Optional {
  def toOptional[T](option: Option[T]): java.util.Optional[T] =
    if (option.isEmpty)
      java.util.Optional.empty()
    else
      java.util.Optional.of(option.get)
}


object MapOps {
  implicit class MapOps[K, +V](map: Map[K,V]) {
    // 2.13 only
    def updatedWithF[V1 >: V](key: K)(remappingFunction: Option[V] => Option[V1]): Map[K,V1] = {
      val previousValue = map.get(key)
      remappingFunction(previousValue) match {
        case None            => previousValue.fold(map)(_ => map - key)
        case Some(nextValue) =>
          if (previousValue.exists(_.asInstanceOf[AnyRef] eq nextValue.asInstanceOf[AnyRef])) map
          else map.updated(key, nextValue)
      }
    }
  }
}