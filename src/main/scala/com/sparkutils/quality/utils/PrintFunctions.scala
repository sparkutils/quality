package com.sparkutils.quality.utils

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.types.DataType

/**
 * Prints the expression generated code with a message, in all other ways it forwards evaluation and compilation to the expression
 *
 * @param child
 * @param msg defaults to "Codegen Result ->"
 */
case class PrintCode(child: Expression, msg: String = "CodeGen Result ->", writer: String => Unit = println(_)) extends UnaryExpression {
  protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)

  def dataType: DataType = child.dataType

  override def eval(input: InternalRow): Any = child.eval(input)

  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val code = child.genCode(ctx)
    writer(
      s"""$msg code is:
${code.code}

value is:
${code.value}

isNull is:
${code.isNull.code}

full context up to this point is:
${ctx.declareMutableStates}
${ctx.declareAddedFunctions}
${ctx.emitExtraCode}
${ctx.initMutableStates}
${ctx.initPartition}
""")
    code
  }
}
