package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.sql.catalyst.expressions.{EquivalentExpressions, Expression, ExpressionEquals}

object QualityExprUtils {

  def currentSubExprState(ctx: CodegenContext): Map[ExpressionEquals, SubExprEliminationState] =
    ctx.subExprEliminationExprs

  def getAllEquivalentExprs(equivalentExpressions: EquivalentExpressions): Seq[Expression] =
    equivalentExpressions.getCommonSubexpressions

  type ExprEquals = ExpressionEquals

  def state(exprCode: ExprCode): SubExprEliminationState =
    SubExprEliminationState(exprCode)

  def addSubExpr(ctx: CodegenContext, expression: Expression, state: SubExprEliminationState): Unit = {
    ctx.subExprEliminationExprs += (ExpressionEquals(expression) -> state)
  }
}
