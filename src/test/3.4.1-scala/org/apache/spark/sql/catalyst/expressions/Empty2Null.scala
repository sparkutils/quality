package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Added for delta tests 2.4.0 needs this, 2.3.0 only really works with 3.3
 * @param child
 */
case class Empty2Null(child: Expression) extends UnaryExpression with String2StringExpression {
  override def convert(v: UTF8String): UTF8String = if (v.numBytes() == 0) null else v

  override def nullable: Boolean = true

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, c => {
      s"""if ($c.numBytes() == 0) {
         |  ${ev.isNull} = true;
         |  ${ev.value} = null;
         |} else {
         |  ${ev.value} = $c;
         |}""".stripMargin
    })
  }

  override protected def withNewChildInternal(newChild: Expression): Empty2Null =
    copy(child = newChild)
}