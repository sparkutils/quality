package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.sql.catalyst.expressions.{EquivalentExpressions, Expression}

object QualityExprUtils {

  def currentSubExprState(ctx: CodegenContext): Map[Expression, SubExprEliminationState] =
    ctx.subExprEliminationExprs

  def getAllEquivalentExprs(equivalentExpressions: EquivalentExpressions): Seq[Expression] =
    equivalentExpressions.getAllEquivalentExprs.filter(_.size > 1).flatten

  type ExprEquals = Expression

  def state(exprCode: ExprCode): SubExprEliminationState =
    SubExprEliminationState(
      exprCode.isNull,
      exprCode.value)

  def addSubExpr(ctx: CodegenContext, expression: Expression, state: SubExprEliminationState): Unit = {
    ctx.subExprEliminationExprs += (expression -> state)
  }

  def evaluateSubExprEliminationState(ctx: CodegenContext, subExprs: SubExprCodes): String = {
    subExprs.codes.mkString("\n")
  }
}
