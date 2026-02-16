package com.sparkutils.quality.impl

import com.sparkutils.quality.QualityException.qualityException
import com.sparkutils.quality.impl.RuleRegistrationFunctions.getBinary
import com.sparkutils.quality.impl.RuleSuiteHelpers.{deserialize, deserializeGroup}
import com.sparkutils.quality.impl.util.CombinedRuleSuiteRows
import com.sparkutils.quality.{RuleSuite, RuleSuiteGroup, rule_suite, rule_suite_group}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, VariableReference}
import org.apache.spark.sql.{Encoder, ShimUtils, SparkSession}
import org.apache.spark.sql.types.{BinaryType, DataType, ObjectType, StructType}
import frameless._
import com.sparkutils.quality.implicits._
import org.apache.spark.sql.types.DataType.equalsIgnoreCaseAndNullability

import scala.reflect.ClassTag

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

/**
 * Ignores extra output expressions
 */
abstract class OfX[T: ClassTag, E: Encoder]() {

  def deserializeIt(bin: Array[Byte]): T
  def create(t: E): T

  private[quality] def attempt(bin: Array[Byte]): Option[T] =
    try {
      Some(deserializeIt(bin))
    } catch {
      case e: Exception => qualityException(s"Could not deserialize a byte array to a ${implicitly[ClassTag[T]].runtimeClass.getSimpleName}", e)
    }

  private[quality] var encoder: ExpressionEncoder[E] = _
  private[quality] val oType = ObjectType(implicitly[ClassTag[T]].runtimeClass)
  private[quality] val enc = implicitly[Encoder[E]]
  private[quality] val valueClass = enc.schema.fields.size == 1 && enc.schema.fields(0).name == "value"
  private[quality] val combinedRowType: DataType = {
    if (valueClass)
      enc.schema.fields(0).dataType
    else
      enc.schema
  }


  def unapply(expression: Any): Option[T] = {
    if (encoder eq null) {
      encoder = ShimUtils.expressionEncoder(enc).resolveAndBind()
    }

    val sameType =
      equalsIgnoreCaseAndNullability(expression.asInstanceOf[Expression].dataType, combinedRowType)

    expression match {
      case e: Literal if e.dataType == oType =>
        Option(e.value).map(_.asInstanceOf[T])
      case e: Literal if e.dataType == BinaryType =>
        attempt(getBinary(e, 0))
      case e: VariableReference if e.dataType == BinaryType =>
        attempt(e.eval().asInstanceOf[Array[Byte]])
      case e: VariableReference if sameType =>
        Some(create(encoder.createDeserializer()(
          if (valueClass)
            InternalRow(e.eval())
          else
            e.eval().asInstanceOf[InternalRow]
        )))
      case _ => None
    }
  }
}

object OfRuleSuiteGroup extends OfX[RuleSuiteGroup, Seq[CombinedRuleSuiteRows]] {

  def deserializeIt(bin: Array[Byte]) = deserializeGroup(bin)
  def create(t:  Seq[CombinedRuleSuiteRows]) = rule_suite_group(t)

}

object OfRuleSuite extends OfX[RuleSuite, CombinedRuleSuiteRows] {

  def deserializeIt(bin: Array[Byte]) = deserialize(bin)
  def create(t:  CombinedRuleSuiteRows) = rule_suite(t)

}
