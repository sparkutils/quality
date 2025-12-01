package com.sparkutils.quality.impl

import com.sparkutils.quality.{DisabledRule, Failed, Passed, Probability, SoftFailed}
import com.sparkutils.shim.expressions.NullIntolerant
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, UnaryExpression}
import org.apache.spark.sql.types.{DataType, DoubleType}

object SoftFailedUtils {
  /**
   * The results must be interpreted and cast back to a double compatible with anyToRuleResult / anyToRuleResultInt
   * @param res
   * @return
   */
  def softFail(res: Any): Double = {
    val ruleRes = RuleLogicUtils.anyToRuleResult(res)
    ruleRes match {
      case Failed | SoftFailed => -1.0
      case Passed => 1.0
      case DisabledRule => -2.0
      case Probability(percentage) => percentage
    }
  }
}

@ExpressionDescription(
  usage = "softfail(expr) - Returns softFailed() when expr evaluates to false or a non true probability.",
  examples = """
    Examples:
      > SELECT softfail(1000 > 2000);
       -1.0
      > SELECT softfail(1000 < 2000);
       1.0
      > SELECT softfail(0.4);
       0.4
  """)
case class SoftFailExpr(child: Expression) extends UnaryExpression with NullIntolerant {

  override def nullSafeEval(res: Any): Any = SoftFailedUtils.softFail(res)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, c => s"com.sparkutils.quality.impl.SoftFailedUtils.softFail($c)")

  override def dataType: DataType = DoubleType

  protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)
}
