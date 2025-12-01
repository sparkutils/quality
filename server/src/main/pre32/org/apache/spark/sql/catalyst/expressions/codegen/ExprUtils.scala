package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.sql.catalyst.expressions.Expression

object QualityExprUtils {

  def currentSubExprState(ctx: CodegenContext): Map[Expression, SubExprEliminationState] =
    ctx.subExprEliminationExprs

}
