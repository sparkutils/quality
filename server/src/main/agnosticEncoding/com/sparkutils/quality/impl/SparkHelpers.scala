package com.sparkutils.quality.impl

import com.sparkutils.quality.QualityException.qualityException
import com.sparkutils.quality.impl.RuleRegistrationFunctions.getBinary
import com.sparkutils.quality.impl.RuleSuiteHelpers.deserialize
import com.sparkutils.quality.impl.util.CombinedRuleSuiteRows
import com.sparkutils.quality.{RuleSuite, rule_suite}
import frameless.TypedEncoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{AgnosticEncoder, ExpressionEncoder}
import org.apache.spark.sql.catalyst.expressions.{Literal, VariableReference}
import org.apache.spark.sql.functions.{lit, named_struct}
import org.apache.spark.sql.{Column, Encoder, Row, ShimUtils, SparkSession}
import org.apache.spark.sql.types.{BinaryType, DataType, StructType}
import scala.reflect.ClassTag

object NamedStruct {
  def apply(pairs: Seq[(String, Column)]): Column =
    named_struct(pairs.flatMap(p => Seq(lit(p._1), p._2)):_*)
}

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

  // TODO need to test this on databricks ASAP
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
