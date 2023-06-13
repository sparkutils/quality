package com.sparkutils.quality.impl.longPair

import com.sparkutils.quality.BloomFilterMap
import com.sparkutils.quality.BloomLookup
import com.sparkutils.quality.impl.bloom.{BloomExpressionLookup, BloomFilterLookup}
import com.sparkutils.quality.impl.id.{GenericLongBasedID, model}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{Column, InputTypeChecks, QualitySparkUtils, SparkSession}

import scala.annotation.tailrec

object LongPair {
  val structType = StructType(Seq(
    StructField("lower", LongType, false),
    StructField("higher", LongType, false)))
}

object LongPairExpression {
  def genRow(input1: Any, input2: Any) = InternalRow(input1, input2)
}

/**
 * Creates RowIDs using two long fields
 * @param left lower long
 * @param right higher long
 */
case class LongPairExpression(left: Expression, right: Expression) extends BinaryExpression with NullIntolerant
  with InputTypeChecks {

  override protected def nullSafeEval(input1: Any, input2: Any): Any = {
    LongPairExpression.genRow(input1, input2)
  }

  override def dataType: DataType = LongPair.structType

  override def sql: String = s"(longPair(${left.sql}, ${right.sql}))"

  override def inputDataTypes: Seq[Seq[DataType]] = Seq(Seq(LongType), Seq(LongType))

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, (input1, input2) =>
      s"com.sparkutils.quality.impl.longPair.LongPairExpression.genRow($input1, $input2)")

  protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}

/**
 * Takes a prefixed lower and upper long pair field, must be 128bit with the provided prefix and converts to lower and higher for other functions
 * @param prefix prefix of the nested fields
 * @param child the id field
 */
case class PrefixedToLongPair(child: Expression, prefix: String) extends UnaryExpression with NullIntolerant
  with InputTypeChecks {

  override protected def nullSafeEval(input1: Any): Any = {
    val i = input1.asInstanceOf[InternalRow]
    InternalRow(i.getLong(1), i.getLong(2))
  }

  override def dataType: DataType = LongPair.structType

  override def sql: String = s"(prefixedToLongPair(${child.sql}))"

  override def inputDataTypes: Seq[Seq[DataType]] = Seq(Seq(GenericLongBasedID(model.ProvidedID, Array.ofDim[Long](2)).dataType(prefix)))

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, (input1) =>
      s"new GenericInternalRow(new Object[]{((InternalRow)$input1).getLong(1), ((InternalRow)$input1).getLong(2)})")

  protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)
}