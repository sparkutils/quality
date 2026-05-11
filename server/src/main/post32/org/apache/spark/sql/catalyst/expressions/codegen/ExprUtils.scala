package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.sql.catalyst.expressions.{EquivalentExpressions, Expression, ExpressionEquals}

object QualityExprUtils {

  def currentSubExprState(ctx: CodegenContext): Map[ExprEquals, SubExprEliminationState] =
    ctx.subExprEliminationExprs

  def getAllEquivalentExprs(equivalentExpressions: EquivalentExpressions): Seq[Expression] =
    equivalentExpressions.getCommonSubexpressions

  type ExprEquals = ExpressionEquals

  def state(exprCode: ExprCode): SubExprEliminationState =
    SubExprEliminationState(exprCode)

  def addSubExpr(ctx: CodegenContext, expression: Expression, state: SubExprEliminationState): Unit = {
    ctx.subExprEliminationExprs += (ExpressionEquals(expression) -> state)
  }

  def evaluateSubExprEliminationState(ctx: CodegenContext, subExprs: SubExprCodes): String = {
    ctx.evaluateSubExprEliminationState(subExprs.states.values)
  }

  // uses real count on spark >=3.2
  def orderedByCount(expressions: Seq[Expression], eq: EquivalentExpressions): Seq[(Expression, Int)] = {
    expressions.map{e =>
      e -> eq.getExprState(e).get.useCount
    }.sortBy(_._2).reverse
  }
}
