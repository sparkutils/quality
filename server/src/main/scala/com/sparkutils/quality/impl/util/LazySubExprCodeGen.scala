package com.sparkutils.quality.impl.util

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator.{JAVA_BOOLEAN, MAX_JVM_METHOD_PARAMS_LENGTH, calculateParamLengthFromExprValues, getLocalInputVariableValues, isValidParamLength, javaType}
import org.apache.spark.sql.catalyst.expressions.{EquivalentExpressions, Expression, ExpressionEquals}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral, JavaCode, ShimExprUtils, SimpleExprValue, SubExprCodes, SubExprEliminationState, TrueLiteral}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

import scala.collection.mutable

object LazySubExprCodeGen extends Logging {

  val initialisationArray = "lazySubExprIArray"
  val isNullArray = "lazySubExprIsNullArray"

  /**
   * SPARK-18395 / apache/spark#15837 revival
   * @param ctx
   * @param expressions
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
    }

    // For some operators, they do not require all its child's outputs to be evaluated in advance.
    // Instead it only early evaluates part of outputs, for example, `ProjectExec` only early
    // evaluate the outputs used more than twice. So we need to extract these variables used by
    // subexpressions and evaluate them before subexpressions.
    val (inputVarsForAllFuncs, exprCodesNeedEvaluate) = commonExprs.map { expr =>
      val (inputVars, exprCodes) = getLocalInputVariableValues(ctx, expr)
      (inputVars.toSeq, exprCodes.toSeq)
    }.unzip

    val needSplit = true//nonSplitCode.map(_.eval.code.length).sum > SQLConf.get.methodSplitThreshold
    val (subExprsMap, exprCodes) = if (needSplit) {
      if (inputVarsForAllFuncs.map(calculateParamLengthFromExprValues).forall(isValidParamLength)) {
        val localSubExprEliminationExprs =
          mutable.HashMap.empty[ExpressionEquals, SubExprEliminationState]

        // forceInline so it's type is not corrupted, must be handled in the SplitCompilation
        def isInitialised(i: Int) = s"$initialisationArray[$i]"
        def setInitialised(i: Int) = s"$initialisationArray[$i] = true;"

        def isNull(i: Int) = s"$isNullArray[$i]"

        commonExprs.zipWithIndex.foreach { case (expr, i) =>
          val eval = ctx.withSubExprEliminationExprs(localSubExprEliminationExprs.toMap) {
            Seq(expr.genCode(ctx))
          }.head

          // Generate the code for this expression tree and wrap it in a function.
          val fnName = ctx.freshName("subExpr")

          val realValue = ctx.addMutableState(javaType(expr.dataType), "subExprValue")

          val inputVars = inputVarsForAllFuncs(i)
          val argList =
            inputVars.map(v => s"${CodeGenerator.typeName(v.javaType)} ${v.variableName}")
          val fn =
            s"""
               |private ${javaType(expr.dataType)} $fnName(${argList.mkString(", ")}) {
               |  if (!${isInitialised(i)}) {
               |    ${eval.code}
               |    ${isNull(i)} = ${eval.isNull};
               |    $realValue = ${eval.value};
               |    ${setInitialised(i)}
               |  }
               |  return $realValue;
               |}
               """.stripMargin

          val inputVariables = inputVars.map(_.variableName).mkString(", ")

          val fnCallName = ctx.addNewFunction(fnName, fn)
          val value = s"${fnCallName}($inputVariables)"

          val isNullFnName = s"${fnName}ForIsNull"

          val isNullFn =
            s"""
               |private boolean $isNullFnName(${argList.mkString(", ")}) {
               |  if (!${isInitialised(i)}) {
               |    $value;
               |  }
               |  return ${isNull(i)};
               |}
           """.stripMargin

          val nullCallName = ctx.addNewFunction(isNullFnName, isNullFn)
          val isNullF = s"${nullCallName}($inputVariables)"

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
      } else {
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
    } else {
      (localSubExprEliminationExprsForNonSplit, Seq.empty)
    }
    SubExprCodes(subExprsMap.toMap, exprCodes.flatten)
  }
}
