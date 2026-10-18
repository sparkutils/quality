package com.sparkutils.quality.impl.util

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.ShimUtils
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator.{ MAX_JVM_METHOD_PARAMS_LENGTH, calculateParamLengthFromExprValues, getLocalInputVariableValues, isValidParamLength, javaType}
import org.apache.spark.sql.catalyst.expressions.{EquivalentExpressions, Expression, ExpressionEquals}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, JavaCode, ShimExprUtils, SimpleExprValue, SubExprCodes, SubExprEliminationState}

import scala.collection.mutable

object LazySubExprCodeGen extends Logging {

  val initialisationArray = "lazySubExprIArray"
  val isNullArray = "lazySubExprIsNullArray"

  /**
   * SPARK-18395 / apache/spark#15837 revival
   * @return
   */
  def lazySubexpressionEliminationForWholeStageCodegen(ctx: CodegenContext, expressions: Seq[Expression]): SubExprCodes = {
    // for oss ctx.subexpressionEliminationForWholeStageCodegen(expressions)
    // Create a clear EquivalentExpressions and SubExprEliminationState mapping
    val equivalentExpressions: EquivalentExpressions = new EquivalentExpressions
    val localSubExprEliminationExprsForNonSplit =
      mutable.HashMap.empty[ExpressionEquals, SubExprEliminationState]

    // Add each expression tree and compute the common subexpressions.
    expressions.foreach(equivalentExpressions.addExprTree(_))

    // Get all the expressions that appear at least twice and set up the state for subexpression
    // elimination.
    val commonExprs = equivalentExpressions.getCommonSubexpressions
/*
    val nonSplitCode = {
      val allStates = mutable.ArrayBuffer.empty[SubExprEliminationState]
      commonExprs.map { expr =>
        ctx.withSubExprEliminationExprs(localSubExprEliminationExprsForNonSplit.toMap) {
          val eval = expr.genCode(ctx)
          // Collects other subexpressions from the children.
          val childrenSubExprs = mutable.ArrayBuffer.empty[SubExprEliminationState]
          expr.foreach { e =>
            ShimExprUtils.currentSubExprState(ctx).get(ExpressionEquals(e)) match {
              case Some(state) => childrenSubExprs += state
              case _ =>
            }
          }
          val state = SubExprEliminationState(eval, childrenSubExprs.toSeq)
          localSubExprEliminationExprsForNonSplit.put(ExpressionEquals(expr), state)
          allStates += state
          Seq(eval)
        }
      }
      allStates.toSeq
    } */

    // For some operators, they do not require all its child's outputs to be evaluated in advance.
    // Instead it only early evaluates part of outputs, for example, `ProjectExec` only early
    // evaluate the outputs used more than twice. So we need to extract these variables used by
    // subexpressions and evaluate them before subexpressions.
    val (inputVarsForAllFuncs, exprCodesNeedEvaluate) = commonExprs.map { expr =>
      val (inputVars, exprCodes) = getLocalInputVariableValues(ctx, expr)
      (inputVars.toSeq, exprCodes.toSeq)
    }.unzip

    val (subExprsMap, exprCodes) =
      if (inputVarsForAllFuncs.map(calculateParamLengthFromExprValues).forall(isValidParamLength)) {
        val localSubExprEliminationExprs =
          mutable.HashMap.empty[ExpressionEquals, SubExprEliminationState]

        commonExprs.zipWithIndex.foreach { case (expr, i) =>
          val eval = ctx.withSubExprEliminationExprs(localSubExprEliminationExprs.toMap) {
            Seq(expr.genCode(ctx))
          }.head

          val inputVars = inputVarsForAllFuncs(i)
          val argList =
            inputVars.map(v => s"${CodeGenerator.typeName(v.javaType)} ${v.variableName}")

          val callParameters = inputVars.map(_.variableName).mkString(", ")

          val (value: String, isNullF: String) =
            generateFunctions(ctx,expr, i, eval, argList, callParameters)

          // Collects other subexpressions from the children.
          val childrenSubExprs = mutable.ArrayBuffer.empty[SubExprEliminationState]
          expr.foreach { e =>
            localSubExprEliminationExprs.get(ExpressionEquals(e)) match {
              case Some(state) => childrenSubExprs += state
              case _ =>
            }
          }

          // initialisation code is done by the compilation
          val code = code""
          val state = SubExprEliminationState(
            ExprCode(code, SimpleExprValue(isNullF, java.lang.Boolean.TYPE), JavaCode.global(value, expr.dataType)),
            childrenSubExprs.toSeq)
          localSubExprEliminationExprs.put(ExpressionEquals(expr), state)
        }
        (localSubExprEliminationExprs, exprCodesNeedEvaluate)
      } else
        // $COVERAGE-OFF$
        {
        // c+p from spark
        val errMsg = "Failed to split subexpression code into small functions because " +
          "the parameter length of at least one split function went over the JVM limit: " +
          MAX_JVM_METHOD_PARAMS_LENGTH
        if (ShimExprUtils.isTesting) {
          throw SparkException.internalError(errMsg)
        } else {
          logInfo(errMsg)
          (localSubExprEliminationExprsForNonSplit, Seq.empty)
        }
        }
        // $COVERAGE-ON$

    SubExprCodes(subExprsMap.toMap, exprCodes.flatten)
  }


  private def generateFunctions(ctx: CodegenContext, expr: Expression, i: Int, eval: ExprCode, argList: Seq[String],
                                callParameters: String) = {

    // forceInline so it's type is not corrupted, must be handled in the SplitCompilation
    def isInitialised(i: Int) = s"$initialisationArray[$i]"

    def setInitialised(i: Int) = s"$initialisationArray[$i] = true;"

    def isNull(i: Int) = s"$isNullArray[$i]"

    // Generate the code for this expression tree and wrap it in a function.
    val fnName = ctx.freshName("subExpr")

    val realValue = ctx.addMutableState(javaType(expr.dataType), "subExprValue")

    val isNullFnName = s"${fnName}ForIsNull"

    val isNullFn =
      s"""
         |private boolean $isNullFnName(${argList.mkString(", ")}) {
         |  if (!${isInitialised(i)}) {
         |    ${eval.code}
         |    ${isNull(i)} = ${eval.isNull};
         |    $realValue = ${eval.value};
         |    ${setInitialised(i)}
         |  }
         |  return ${isNull(i)};
         |}
           """.stripMargin

    val nullCallName = ctx.addNewFunction(isNullFnName, isNullFn)
    val nullCall = s"$nullCallName($callParameters)"

    val fn =
      s"""
         |private ${javaType(expr.dataType)} $fnName(${argList.mkString(", ")}) {
         |  $nullCall;
         |  return $realValue;
         |}
       """.stripMargin

    val fnCallName = ctx.addNewFunction(fnName, fn)
    val initValueCall = s"$fnCallName($callParameters)"

    (initValueCall, nullCall)
  }

  /**
   * Based on the Spark subexpressionElimination code.  Following the approach from Spark any statefuls are
   * copied before codegen.
   * This, alongside forcing initialise to be called, as part of #131 require the test cases to change.
   */
  def lazyNonWholeStageSubexpressionElimination(ctx: CodegenContext,
                                                dirtyExpressions: Seq[Expression]): String = {
    val expressions = ShimUtils.copyStateful(dirtyExpressions)
    import ctx._
    val equivalentExpressions: EquivalentExpressions = new EquivalentExpressions

    // Add each expression tree and compute the common subexpressions.
    expressions.foreach(equivalentExpressions.addExprTree(_))
    // there are no functions to call in lazy mode
    val subexprFunctions = ""
    // Get all the expressions that appear at least twice and set up the state for subexpression
    // elimination.
    val commonExprs = ShimExprUtils.getAllEquivalentExprs(equivalentExpressions)
    commonExprs.zipWithIndex.foreach { case (expr, i) =>
      // Generate the code for this expression tree and wrap it in a function.
      val eval = expr.genCode(ctx)

      val (value: String, isNullF: String) =
        generateFunctions(ctx,expr, i, eval, Seq(s"InternalRow $INPUT_ROW"), INPUT_ROW)

      // Add a state and a mapping of the common subexpressions that are associate with this
      // state. Adding this expression to subExprEliminationExprMap means it will call `fn`
      // when it is code generated. This decision should be a cost based one.
      //
      // The cost of doing subexpression elimination is:
      //   1. Extra function call, although this is probably *good* as the JIT can decide to
      //      inline or not.
      // The benefit doing subexpression elimination is:
      //   1. Running the expression logic. Even for a simple expression, it is likely more than 3
      //      above.
      //   2. Less code.
      // Currently, we will do this for all non-leaf only expression trees (i.e. expr trees with
      // at least two nodes) as the cost of doing it is expected to be low.

      val state = ShimExprUtils.state(
        ExprCode(code"", SimpleExprValue(isNullF, java.lang.Boolean.TYPE), JavaCode.global(value, expr.dataType))
      )
      ShimExprUtils.addSubExpr(ctx, expr, state)
    }

    subexprFunctions
  }

}
