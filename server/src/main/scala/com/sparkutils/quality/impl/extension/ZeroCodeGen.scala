package com.sparkutils.quality.impl.extension

import org.apache.spark.internal.Logging
import org.apache.spark.sql.ShimUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, UnaryExpression, Unevaluable}
import org.apache.spark.sql.types.DataType

/**
 * Cannot be used as part of the initial expression trees, but can be used to swap out a runner to reduce
 * compilation time and size.  Added as part of #129 as Databricks could not generate a 20k rulesuite, despite #131's
 * separate compilation, the common subexpressions were already too much for databricks.
 */
case class ZeroCodeGen(child: Expression, realChild: Expression, on32: Boolean = false, wrapped: Boolean = false) extends UnaryExpression with Logging {

  override def nullable: Boolean = realChild.nullable

  override def eval(input: InternalRow): Any = realChild.eval(input)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    realChild.genCode(ctx)
  }

  override lazy val canonicalized: Expression = realChild.canonicalized

  override def dataType: DataType = realChild.dataType

  protected def withNewChildInternal(newChild: Expression): Expression =
    if ((!wrapped && newChild.collect{
      case u: Unevaluable => u
      case s if ShimUtils.isStateful(s) => s  // stateful can't be known in advance so it must be handled here
    }.nonEmpty) || on32) // remove in 0.3.0 when we drop 3.2, 3.2 puts the apply call into a subexpression and doesn't pass params as well Literal doesn't have any
      copy(child = newChild, realChild = newChild)
    else  // hopefully late enough to not be an issue to swap a nullable, the below match is a safeguard
      if (wrapped) {
        // $COVERAGE-OFF$
        logTrace("ZeroCodeGen doesn't expect another expression optimisation after Unevaluable's are removed but got "+newChild)
        copy(child = newChild)
        // $COVERAGE-ON$
      } else {
        copy(child = Literal(null, newChild.dataType), realChild = newChild, wrapped = true)
      }

}
