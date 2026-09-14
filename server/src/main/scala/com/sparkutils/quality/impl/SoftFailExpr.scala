package com.sparkutils.quality.impl

import com.sparkutils.quality.impl.RuleSuiteHelpers.ruleResultToInt
import com.sparkutils.quality.{DisabledRule, Failed, FailedInt, Passed, PassedInt, Probability, SoftFailed, SoftFailedInt}
import com.sparkutils.shim.expressions.NullIntolerant
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, UnaryExpression}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType}

object SoftFailedUtils {
  /**
   * Passed and Failed need special handling, all other values pass through their in values
   * @param res
   * @return
   */
  def softFail(res: Any): Int = {
    val ruleRes = RuleLogicUtils.anyToRuleResultInt(res)
    ruleRes match {
      case FailedInt => -1
      case PassedInt => 1
      case _ => ruleRes
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

  override def dataType: DataType = IntegerType

  protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)
}
