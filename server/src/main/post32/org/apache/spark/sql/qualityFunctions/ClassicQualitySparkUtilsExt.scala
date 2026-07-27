package org.apache.spark.sql.qualityFunctions

import com.sparkutils.quality.impl.SplitCompilation
import com.sparkutils.quality.impl.extension.ZeroCodeGen
import com.sparkutils.quality.impl.util.{ParameterInformation, SeparateCompilation}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, ExpressionEquals, ExpressionProxy}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, EmptyBlock, ExprCode, ExprValue, JavaCode, SubExprEliminationState, VariableValue}

import scala.collection.mutable

object ClassicQualitySparkUtilsExt {

  def collectFirstZeroCodeGenSplitCompilations(ctx: CodegenContext, expr: Seq[Expression]): Seq[SplitCompilation] = {
    def firstZeroWith(expression: Expression): Seq[SplitCompilation] =
      expression match {
        case ZeroCodeGen(_, s: SplitCompilation, _, _) =>
          // ensure it has been compiled
          expression.genCode(ctx)
          // do not go further
          Seq(s)
        case p: ExpressionProxy =>
          firstZeroWith(p.child)
        case _ => expression.children.flatMap(firstZeroWith)
      }
    expr.flatMap(firstZeroWith)
  }

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

    val splits = collectFirstZeroCodeGenSplitCompilations(ctx, expr)
    val totalParams =
      splits.foldLeft(ParameterInformation.forMerging) {
        (cur, s) =>
          val p = s.usedParameters_
          cur.mergeParams(ctx, p, false)
      }

    totalParams.params.foreach{
      p =>
        argSet += JavaCode.variable(p.name, p.classType)
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
