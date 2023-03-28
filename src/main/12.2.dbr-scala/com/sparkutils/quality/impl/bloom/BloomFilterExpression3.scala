package com.sparkutils.quality.impl.bloom

import com.sparkutils.quality.impl.longPair.SaferLongPairsExpression
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

object BloomFilterLookupSparkVersionSpecific {

  /**
   * Identifies blooms from either a resolved or unresolved expression
   *
   * @param expression a single Expression, can be a complex tree or simple direct probabilityIn or rowid
   * @return The bloom id's used, for unresolved expression trees this may contain blooms which are not present in the bloom map
   */
  def getBlooms(expression: Expression): Seq[String] = {
    val res = Seq.empty

    def bloom(expression: Expression, acc: Seq[String]): Seq[String] = {

      // case one
      expression match {
        case UnresolvedFunction( Seq("probabilityIn" | "rowid") , Seq(left, Literal(id: UTF8String, StringType)), _, _, _) => bloom(left, acc :+ id.toString)
        case BloomFilterLookupExpression(left, Literal(id: UTF8String, StringType), _) => bloom(left, acc :+ id.toString)
        case SaferLongPairsExpression(left, Literal(id: UTF8String, StringType), _) => bloom(left, acc :+ id.toString)
        case e : Expression => e.children.foldLeft(acc) { (acc, e) => bloom(e, acc) }
        case _ => acc
      }
    }
    bloom(expression, res)
  }

}
