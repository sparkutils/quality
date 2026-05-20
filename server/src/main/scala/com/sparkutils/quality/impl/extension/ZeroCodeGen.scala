package com.sparkutils.quality.impl.extension

import com.sparkutils.quality.groupProcessorKey
import com.sparkutils.quality.impl.{Runner, Triggers}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, UnaryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.DataType

/**
 * Cannot be used as part of the initial expression trees, but can be used to swap out a runner to reduce
 * compilation time and size.  Added as part of #129 as Databricks could not generate a 20k rulesuite, despite #131's
 * separate compilation, the common subexpressions were already too much for databricks.
 * @param realChild
 */
case class ZeroCodeGen(child: Expression, realChild: Expression) extends UnaryExpression {

  override def nullable: Boolean = realChild.nullable

  override def eval(input: InternalRow): Any = realChild.eval(input)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    realChild.genCode(ctx)
  }

  override def dataType: DataType = realChild.dataType

  override protected def withNewChildInternal(newChild: Expression): Expression =
    if (newChild.children.exists(c => c.exists(_.isInstanceOf[Unevaluable])))
      copy(child = newChild, realChild = newChild)
    else
      copy(child = Literal(null, newChild.dataType), realChild = newChild) // hopefully late enough to not be an issue to swap a nullable
}

/**
 * Swaps out runners with over 400 rules (800 actual expressions) or extraConfig has defined a trigger grouping
 */
object ZeroCodeGenRule extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan.transformAllExpressions {
      case r: Runner if (r.children.size > 800 || Triggers.getValue(groupProcessorKey, r.extraConfig, "").nonEmpty)
        && !r.alreadyZero =>
        val nr = r.withZeroCode()
        ZeroCodeGen(nr, nr)
    }
}
