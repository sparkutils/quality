package com.sparkutils.quality.impl

import com.sparkutils.quality.QualityException.qualityException
import com.sparkutils.quality.impl.RuleRegistrationFunctions.getBinary
import com.sparkutils.quality.impl.RuleSuiteHelpers.deserialize
import com.sparkutils.quality.impl.util.CombinedRuleSuiteRows
import com.sparkutils.quality.{RuleSuite, rule_suite}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Literal, VariableReference}
import org.apache.spark.sql.{Encoder, ShimUtils, SparkSession}
import org.apache.spark.sql.types.{BinaryType,  StructType}

/**
 * Ignores extra output expressions
 */
object OfRuleSuite {

  private[quality] def attempt(bin: Array[Byte]): Option[RuleSuite] =
    try {
      Some(deserialize(bin))
    } catch {
      case e: Exception => qualityException("Could not deserialize a byte array to a RuleSuite", e)
    }

  private[quality] var combinedRowType: StructType = _
  private[quality] var encoder: ExpressionEncoder[CombinedRuleSuiteRows] = _

  def unapply(expression: Any): Option[RuleSuite] = {
    if (combinedRowType eq null) {
      val s = SparkSession.active
      import s.implicits._
      val enc = implicitly[Encoder[CombinedRuleSuiteRows]]
      encoder = ShimUtils.expressionEncoder(enc).resolveAndBind()
      combinedRowType = enc.schema
    }

    expression match {
      case e: Literal if e.dataType == BinaryType =>
        attempt(getBinary(e, 0))
      case e: VariableReference if e.dataType == BinaryType =>
        attempt(e.eval().asInstanceOf[Array[Byte]])
      case e: VariableReference if e.dataType == combinedRowType =>
        Some(rule_suite(encoder.createDeserializer()(e.eval().asInstanceOf[InternalRow])))
      case _ => None
    }
  }
}

/**
 * Requires output expressions
 */
object OfRuleOutputSuite {
  import OfRuleSuite.attempt

  def unapply(expression: Any): Option[RuleSuite] = OfRuleSuite.unapply(expression)
  /*  expression match {
      case e: Literal if e.dataType == BinaryType =>
        attempt(getBinary(e, 0))
      case e: VariableReference if e.dataType == BinaryType =>
        attempt(e.eval().asInstanceOf)
      case _ => None
    }*/
}
