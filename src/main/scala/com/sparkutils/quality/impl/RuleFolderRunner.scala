package com.sparkutils.quality.impl

import com.sparkutils.quality._
import com.sparkutils.quality.impl.imports.RuleFolderRunnerImports
import com.sparkutils.quality.impl.util.{NonPassThrough, PassThrough}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, NonSQLExpression}
import org.apache.spark.sql.qualityFunctions.{FunN, RefExpressionLazyType}
import org.apache.spark.sql.types._

import java.util.concurrent.atomic.AtomicReference



private[quality] object RuleFolderRunnerUtils extends RuleFolderRunnerImports {

  /**
   * Needs sorting in salience order for output processing but NOT for result's.
   * So output array should use offsets, results should be offset, but order
   * of calling must be salience based.
   *
   */

  def compiledEvalDebug[T](results: InternalRow, output: T): InternalRow =
    InternalRow(results, output)

  def compiledEval[T](results: InternalRow, currentSalience: Int, rules: Array[(Long, Long, Long)], currentOutputIndex: Int, output: Array[T]): InternalRow =
    InternalRow(results,
      if (currentSalience == java.lang.Integer.MAX_VALUE)
        null
      else output(currentOutputIndex)
    )

}

/**
  * Children will be rewritten by the plan, it's then re-incorporated into ruleSuite
  * expressionOffsets.length is the length of the trigger expressions in realChildren, realChildren(expressionOffsets.length + expressionOffsets(x)) will be the correct OutputExpression
  */
case class RuleFolderRunner(ruleSuite: RuleSuite, left: Expression, right: Expression, resultDataType: () => DataType,
                            compileEvals: Boolean, debugMode: Boolean, variablesPerFunc: Int,
                            variableFuncGroup: Int, forceRunnerEval: Boolean, expressionOffsets: Array[Int],
                            dataRef: AtomicReference[DataType], forceTriggerEval: Boolean) extends BinaryExpression with NonSQLExpression with CodegenFallback {

  // hack to push type through to lambda's, resolution only happens on driver but only works with a projection e.g. withColumn or introducing an extra column
  if (left.resolved) {
    dataRef.set(left.dataType)
  }

  import RuleEngineRunnerUtils._
  import RuleFolderRunnerUtils._

  val startingStruct: Expression = left

  lazy val realChildren =
    right match {
      case r @ NonPassThrough(_) => r.rules
      case PassThrough(children) => children
    }

  // only used for compilation
  lazy val compiledRealChildren = realChildren.slice(0, expressionOffsets.length).map(ExpressionWrapper(_, compileEvals)).toArray

  override def nullable: Boolean = false
  override def toString: String = s"RuleFolderRunner(${realChildren.mkString(", ")})"

  // used only for eval, compiled uses the children directly
  lazy val reincorporated = reincorporateExpressions(ruleSuite, realChildren, compileEvals, expressionOffsets)

  // keep it simple for this one. - can return an internal row or whatever..
  override def eval(input: InternalRow): Any = {
    val starter = startingStruct.eval(input).asInstanceOf[InternalRow] // TODO - throw a decent error message at ruleFolder call
    val (res, processedRes) = //(null, null)
      RuleSuiteFunctions.foldWithProcessors(reincorporated, input, starter, debugMode)
    InternalRow(com.sparkutils.quality.impl.RuleRunnerUtils.ruleResultToRow(res), processedRes)
  }

  def dataType: DataType = StructType( Seq(
      StructField(name = "ruleSuiteResults", dataType = com.sparkutils.quality.types.ruleSuiteResultType),
      StructField(name = "result", dataType = resultDataType(), nullable = true)
    ))

  override protected def doGenCode(ctx:  _root_.org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext, ev:  _root_.org.apache.spark.sql.catalyst.expressions.codegen.ExprCode): _root_.org.apache.spark.sql.catalyst.expressions.codegen.ExprCode = {
    if (forceRunnerEval) {
      return super[CodegenFallback].doGenCode(ctx, ev)
    }

    ctx.references += this

    // need to setup the folder variable to pass around, create it with "left"
    // thread it through
    val folderV = ctx.addMutableState( "InternalRow",
      ctx.freshName("folderV") )

    val ruleRunnerExpressionIdx = ctx.references.size - 1
    val ruleRunnerClassName = classOf[RuleFolderRunner].getName
    val funName = classOf[FunN].getName
    val lazyRefName = classOf[RefExpressionLazyType].getName

    // order by salience
    val salience = com.sparkutils.quality.impl.RuleEngineRunnerUtils.flattenSalience(ruleSuite)
    val outputs = 0 until (realChildren.size - expressionOffsets.size)
    val reordered = outputs zip salience sortBy(_._2) map(_._1)

    val offsetTerm = ctx.addMutableState("int", ctx.freshName("offset"),
      v => s"$v = ${expressionOffsets.size};")

    val compilerTerms =
      RuleEngineRunnerUtils.genCompilerTerms[RuleFolderRunner](ctx, right, expressionOffsets, realChildren,
        debugMode, variablesPerFunc, variableFuncGroup, forceTriggerEval,
        // capture the current
        extraResult = (outArrTerm: String) => s"$folderV = $outArrTerm;",
        extraSetup = (idx: String) =>
          // set the current
          s"(($lazyRefName)(($funName)(($ruleRunnerClassName)references[$ruleRunnerExpressionIdx]).realChildren().apply($offsetTerm + $idx)).arguments().apply(0)).value_$$eq($folderV);",
        orderOffset = (idx: Int) => reordered(idx),
        // we shouldn't check salience as we are already ordered by it
        salienceCheck = false
      ).getOrElse(return super[CodegenFallback].doGenCode(ctx, ev))

    import compilerTerms._

    // generate the starting struct
    val starterEval = left.genCode(ctx)

    val pre = s"""
          $currentSalience = java.lang.Integer.MAX_VALUE;
          $currentOutputIndex = -1;

          // starting
          ${starterEval.code}
          // setting the folder
          $folderV = ${starterEval.isNull} ? null : (InternalRow)${starterEval.value}; \n

          ${funNames.map{f => s"$f($paramsCall);"}.mkString("\n")}
      """
    val post = s"""

          boolean ${ev.isNull} = false;
      """

    val res =
      if (debugMode)
        ev.copy(code = code"""
          $pre

          InternalRow ${ev.value} =
            com.sparkutils.quality.impl.RuleFolderRunnerUtils.compiledEvalDebug(
              $utilsName.evalArray($ruleSuitTerm, $ruleSuiteArrays, $resArrTerm),
            ($currentOutputIndex < 0) ? null : com.sparkutils.quality.impl.RuleEngineRunnerUtils.debugOutput($salienceArrTerm, $outArrTerm, $currentOutputIndex));

          $post
          """
        )
      else
        ev.copy(code = code"""
          $pre

          InternalRow ${ev.value} =
            com.sparkutils.quality.impl.RuleFolderRunnerUtils.compiledEval(
              $utilsName.evalArray($ruleSuitTerm, $ruleSuiteArrays, $resArrTerm),
              $currentSalience, $ruleTupleArrTerm, $currentOutputIndex, $outArrTerm);

          $post
          """
        )

    res

  }

  protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = copy(left = newLeft, right = newRight)
}
