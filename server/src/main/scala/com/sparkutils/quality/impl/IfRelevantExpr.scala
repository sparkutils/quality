package com.sparkutils.quality.impl

import com.sparkutils.quality.impl.RuleLogicUtils.{TRUE_INT, anyToRuleResultIntGen}
import com.sparkutils.quality.impl.imports.RuleResultsImports.{FailedInt, IgnoredRuleInt, PassedInt}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionDescription}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.types.{DataType, IntegerType}

@ExpressionDescription(
  usage = "if_relevant(filter, cond) - Returns passed if filter and cond are true, failed if filter is true and cond is false or null, ignoredRule if filter is false, failed if filter is null.",
  examples = """
    Examples:
      > SELECT if_relevant(id > 0, id % 2 = 0);
      Returns Passed for even positive ids, Failed for odd positive ids, IgnoredRule for non-positive ids.
      > SELECT if_relevant(true, false);
      Returns Failed.
  """
)
case class IfRelevantExpr(left: Expression, right: Expression) extends BinaryExpression {

  override val nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val leftValue = left.eval(input)
    if (leftValue == null) FailedInt
    else if (RuleLogicUtils.anyToRuleResultInt(leftValue) != PassedInt)
      IgnoredRuleInt
    else {
      val rightValue = right.eval(input)
      if (rightValue == null) FailedInt
      else {
        val r = RuleLogicUtils.anyToRuleResultInt(rightValue)
        if (r == PassedInt) TRUE_INT else r
      }
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val leftCode = left.genCode(ctx)
    val rightCode = right.genCode(ctx)
    val ifRelevantFilterRes = ctx.freshName("ifRelevantFilterRes")
    val ifRelevantCondRes = ctx.freshName("ifRelevantCondRes")

    ev.copy(isNull = FalseLiteral, code =
      code"""
         |${leftCode.code}
         |int ${ev.value} = $FailedInt;
         |if (!${leftCode.isNull}) {
         |  int $ifRelevantFilterRes = ${anyToRuleResultIntGen(leftCode.value, leftCode.isNull)};
         |  if ($ifRelevantFilterRes != $PassedInt) {
         |    ${ev.value} = $IgnoredRuleInt;
         |  } else {
         |    ${rightCode.code}
         |    if (!${rightCode.isNull}) {
         |      int $ifRelevantCondRes = ${anyToRuleResultIntGen(rightCode.value, rightCode.isNull)};
         |      ${ev.value} = ($ifRelevantCondRes == $PassedInt) ? $TRUE_INT : $ifRelevantCondRes;
         |    }
         |  }
         |}
         |""".stripMargin
    )
  }

  override val dataType: DataType = IntegerType

  protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
}
