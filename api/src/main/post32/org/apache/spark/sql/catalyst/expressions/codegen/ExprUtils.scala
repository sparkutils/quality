package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.sql.catalyst.expressions.ExpressionEquals

object QualityExprUtils {

  def currentSubExprState(ctx: CodegenContext): Map[ExpressionEquals, SubExprEliminationState] =
    ctx.subExprEliminationExprs

}
