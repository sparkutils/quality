package com.sparkutils.quality.impl

import com.sparkutils.quality.impl.RuleLogicUtils.anyToRuleResultIntGen
import com.sparkutils.quality.impl.imports.RuleResultsImports.PassedInt
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.types.{BooleanType, DataType}

/**
 * When the value is compatible with PassedInt (or TRUE_INT) true, otherwise false
 * @param child
 */
case class PassedTestExpr(child: Expression) extends UnaryExpression {

  override val nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val leftValue = child.eval(input)
    if (leftValue == null) false
    else if (RuleLogicUtils.anyToRuleResultInt(leftValue) == PassedInt)
      true
    else
      false
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val childCode = child.genCode(ctx)

    ev.copy(isNull = FalseLiteral, code =
      code"""
         |${childCode.code}
         |boolean ${ev.value} = false;
         |if (!${childCode.isNull} && ${anyToRuleResultIntGen(childCode, compareToPassedInt = true)}) {
         |  ${ev.value} = true;
         |}
         |""".stripMargin
    )
  }

  override val dataType: DataType = BooleanType

  protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

object PassedTestExpr {

  def passed(what: Expression): Expression = PassedTestExpr(what)

}