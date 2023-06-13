package com.sparkutils.quality.impl.longPair

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression}
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

object AsUUID {
  def apply(lower: Column, higher: Column): Column =
    new Column(AsUUID(lower.expr, higher.expr))
}

/**
 * Converts a lower and higher pair of longs into a uuid string
 * @param left
 * @param right
 */
case class AsUUID(left: Expression, right: Expression) extends BinaryExpression {
  protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = copy(left = newLeft, right = newRight)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, (lower, higher) => s"org.apache.spark.unsafe.types.UTF8String.fromString(new java.util.UUID($higher, $lower).toString())")

  override def dataType: DataType = StringType

  override protected def nullSafeEval(lower: Any, higher: Any): Any =
    UTF8String.fromString(new java.util.UUID(higher.asInstanceOf[Long], lower.asInstanceOf[Long]).toString)
}
