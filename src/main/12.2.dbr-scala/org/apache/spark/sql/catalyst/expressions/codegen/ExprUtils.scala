package org.apache.spark.sql.catalyst.expressions.codegen

import com.sparkutils.quality.impl.util.Params.stripBrackets
import org.apache.spark.sql.catalyst.expressions.ExpressionEquals

object ExprUtils {

  def currentSubExprState(ctx: CodegenContext): Map[ExpressionEquals, SubExprEliminationState] =
    ctx.subExprEliminationExprs

  def isVariableMutableArray(ctx: CodegenContext, variable: VariableValue): Boolean =
    variable.javaType.isArray && ctx.arrayCompactedMutableStates.
      get(s"${variable.javaType.getComponentType.getName}[]").exists(_.arrayNames.contains(stripBrackets(variable)._1))
}
