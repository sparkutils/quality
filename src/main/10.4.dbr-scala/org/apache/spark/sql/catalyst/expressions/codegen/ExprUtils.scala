package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.sql.catalyst.expressions.ExpressionEquals

object ExprUtils {

  def stripBrackets(v: VariableValue): String =
    if (v.javaType.isArray) {
      val openb = v.toString().indexOf("[")
      v.toString().dropRight(v.length - openb)
    } else
      v.toString()

  def currentSubExprState(ctx: CodegenContext): Map[ExpressionEquals, SubExprEliminationState] =
    ctx.subExprEliminationExprs

  def isVariableMutableArray(ctx: CodegenContext, variable: VariableValue): Boolean =
    variable.javaType.isArray && ctx.arrayCompactedMutableStates.
      get(s"${variable.javaType.getComponentType.getName}[]").exists(_.arrayNames.contains(stripBrackets(variable)))
}
