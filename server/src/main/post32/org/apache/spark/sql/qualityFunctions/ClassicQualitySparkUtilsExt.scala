package org.apache.spark.sql.qualityFunctions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, ExpressionEquals}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, EmptyBlock, ExprCode, ExprValue, JavaCode, SubExprEliminationState, VariableValue}

import scala.collection.mutable

object ClassicQualitySparkUtilsExt {

  // based on Spark 4.1 CodeGenerator.getLocalInputVariableValues
  def getLocalInputVariableValues(
                                   ctx: CodegenContext,
                                   expr: Seq[Expression],
                                   subExprs: Map[ExpressionEquals, SubExprEliminationState])
  : (Set[VariableValue], Set[ExprCode]) = {
    val argSet = mutable.Set[VariableValue]()
    val exprCodesNeedEvaluate = mutable.Set[ExprCode]()

    if (ctx.INPUT_ROW != null) {
      argSet += JavaCode.variable(ctx.INPUT_ROW, classOf[InternalRow])
    }

    // Collects local variables from a given `expr` tree
    val collectLocalVariable = (ev: ExprValue) => ev match {
      case vv: VariableValue => argSet += vv
      case _ =>
    }

    val stack = mutable.Stack[Expression]()
    stack.pushAll(expr)
    while (stack.nonEmpty) {
      stack.pop() match {
        case ref: BoundReference if ctx.currentVars != null &&
          ctx.currentVars(ref.ordinal) != null =>
          val exprCode = ctx.currentVars(ref.ordinal)
          // If the referred variable is not evaluated yet.
          if (exprCode.code != EmptyBlock) {
            exprCodesNeedEvaluate += exprCode.copy()
            exprCode.code = EmptyBlock
          }
          collectLocalVariable(exprCode.value)
          collectLocalVariable(exprCode.isNull)

        case e =>
          subExprs.get(ExpressionEquals(e)) match {
            case Some(state) =>
              collectLocalVariable(state.eval.value)
              collectLocalVariable(state.eval.isNull)
            case None =>
              stack.pushAll(e.children)
          }
      }
    }

    (argSet.toSet, exprCodesNeedEvaluate.toSet)
  }
}
