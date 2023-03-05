package com.sparkutils.quality.impl

import com.sparkutils.quality.utils.Serializing
import com.sparkutils.quality.{Passed, RuleLogicUtils, SoftFailedInt}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.types.{DataType, IntegerType}

object SoftFailedUtils {
  def softFail(res: Any): Integer = {
    val ruleRes = RuleLogicUtils.anyToRuleResult(res)
    if (ruleRes != Passed)
      SoftFailedInt
    else
      Serializing.ruleResultToInt(ruleRes)
  }
}

@ExpressionDescription(
  usage = "softfail(expr) - Returns softFailed() when expr evaluates to false or a non true probability.",
  examples = """
    Examples:
      > SELECT softfail(1000 > 2000);
       -1
  """)
case class SoftFailExpr(child: Expression) extends UnaryExpression with NullIntolerant {

  override def nullSafeEval(res: Any): Any = SoftFailedUtils.softFail(res)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, c => s"com.sparkutils.quality.impl.SoftFailedUtils.softFail($c)")

  override def nullable: Boolean = false

  override def dataType: DataType = IntegerType

  protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)
}
