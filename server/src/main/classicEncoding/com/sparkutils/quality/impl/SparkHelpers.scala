package com.sparkutils.quality.impl

import com.sparkutils.quality.QualityException.qualityException
import com.sparkutils.quality.RuleSuite
import com.sparkutils.quality.impl.RuleRegistrationFunctions.getBinary
import frameless.TypedEncoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types.{BinaryType, DataType}

import scala.reflect.ClassTag

object Encoders extends EncodersImplicits {

  def internalRowTypedEnc(rowType: DataType): TypedEncoder[InternalRow] =
    new TypedEncoder[InternalRow]()(ClassTag(classOf[InternalRow])) {
      def nullable: Boolean = true

      def jvmRepr: DataType = rowType
      def catalystRepr: DataType = rowType

      def fromCatalyst(path: Expression): Expression = path

      def toCatalyst(path: Expression): Expression = path
    }

}

/**
 * Ignores extra output expressions
 */
object OfRuleSuite {

  private[quality] def attempt(bin: Array[Byte]): Option[RuleSuite] =
    try {
      Some(RuleSuiteHelpers.deserialize(bin))
    } catch {
      case e: Exception => qualityException("Could not deserialize a byte array to a RuleSuite", e)
    }

  def unapply(expression: Any): Option[RuleSuite] =
    expression match {
      case e: Literal if e.dataType == BinaryType =>
        attempt(getBinary(e, 0))
      case _ => None
    }
}

/**
 * Requires output expressions
 */
object OfRuleOutputSuite {

  def unapply(expression: Any): Option[RuleSuite] = OfRuleSuite.unapply(expression)

}