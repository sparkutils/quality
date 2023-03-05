package com.sparkutils.quality.impl

import com.sparkutils.quality.impl.RuleRunnerUtils.RuleSuiteResultArray
import com.sparkutils.quality.utils.NonPassThrough
import com.sparkutils.quality.{ExprLogic, ExpressionWrapper, Id, NoOpRunOnPassProcessor, RuleLogic, RuleSuite, RunOnPassProcessor}
import com.sparkutils.quality.QualityException.qualityException
import com.sparkutils.quality.impl.RuleEngineRunnerUtils.flattenExpressions
import com.sparkutils.quality.impl.RuleRunnerUtils.{RuleSuiteResultArray, genRuleSuiteTerm, packTheId}
import com.sparkutils.quality.utils.{NonPassThrough, PassThrough}
import com.sparkutils.quality._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression, UnaryExpression}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, QualitySparkUtils}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

trait RuleEngineRunnerImports {

  /**
   * Creates a column that runs the RuleSuite.  This also forces registering the lambda functions used by that RuleSuite
   * @param ruleSuite The ruleSuite with runOnPassProcessors
   * @param resultDataType The type of the results from runOnPassProcessors - must be the same for all result types
   * @param compileEvals Should the rules be compiled out to interim objects - by default true for eval usage, wholeStageCodeGen will evaluate in place unless forceTriggerEval set to false
   * @param debugMode When debugMode is enabled the resultDataType is wrapped in Array of (salience, result) pairs to ease debugging
   * @param resolveWith This experimental parameter can take the DataFrame these rules will be added to and pre-resolve and optimise the sql expressions, see the documentation for details on when to and not to use this.
   * @param variablesPerFunc Defaulting to 40 allows, in combination with variableFuncGroup allows customisation of handling the 64k jvm method size limitation when performing WholeStageCodeGen
   * @param variableFuncGroup Defaulting to 20
   * @param forceRunnerEval Defaulting to false, passing true forces a simplified partially interpreted evaluation (compileEvals must be false to get fully interpreted)
   * @param forceTriggerEval Defaulting to true, passing true forces each trigger expression to be compiled (compileEvals) and used in place, false instead expands the trigger in-line giving possible performance boosts based on JIT.  Most testing has however shown this not to be the case hence the default, ymmv.
   * @return A Column representing the QualityRules expression built from this ruleSuite
   */
  def ruleEngineRunner(ruleSuite: RuleSuite, resultDataType: DataType, compileEvals: Boolean = true,
                       debugMode: Boolean = false, resolveWith: Option[DataFrame] = None, variablesPerFunc: Int = 40,
                       variableFuncGroup: Int = 20, forceRunnerEval: Boolean = false, forceTriggerEval: Boolean = true): Column = {
    com.sparkutils.quality.registerLambdaFunctions( ruleSuite.lambdaFunctions )
    val realType =
      if (debugMode)
        // wrap it in an array with the priority result
        ArrayType(StructType(Seq(StructField("salience", IntegerType), StructField("result", resultDataType))))
      else
        resultDataType

    val (expressions, indexes) = flattenExpressions(ruleSuite)

    val runner = new RuleEngineRunner(ruleSuite, PassThrough( expressions ), realType, compileEvals,
      debugMode, variablesPerFunc, variableFuncGroup, forceRunnerEval, expressionOffsets = indexes, forceTriggerEval)

    new Column(
      QualitySparkUtils.resolveWithOverride(resolveWith).map { df =>
        val resolved = QualitySparkUtils.resolveExpression(df, runner)

        resolved.asInstanceOf[RuleEngineRunner].copy(child = resolved.children(0) match {
          // replace the expr
          case PassThrough(children) => NonPassThrough(children)
        })
      } getOrElse runner
    )
  }
}

private[quality] object RuleEngineRunnerUtils extends RuleEngineRunnerImports {

  protected[quality] def flattenExpressions(ruleSuite: RuleSuite, transformOutputExpression: Expression => Expression = identity): (Seq[Expression], Array[Int]) = {
    val outputs = mutable.Map.empty[Id, Int]
    var pos = 0
    val outputExpressions = new mutable.ArrayBuffer[Expression](10)
    val indexes = new mutable.ArrayBuffer[Int](300)

    val expressions =
      ruleSuite.ruleSets.flatMap( ruleSet => ruleSet.rules.map(rule => {
        val expr =
          rule.expression match {
            case r: ExprLogic => r.expr // only ExprLogic are possible here
          }

        val idx = outputs.getOrElse(rule.runOnPassProcessor.id, {
            val expr = rule.runOnPassProcessor match {
              case NoOpRunOnPassProcessor.noOp => qualityException(s"You cannot use a RuleEngineRunner if any of the rules do not have RunOnPassProcessors set ruleSet ${ruleSet.id}, rule ${rule.id}}")
              case r: RunOnPassProcessor => r.returnIfPassed.expr
            }
            outputs.put(rule.runOnPassProcessor.id, pos)

            outputExpressions += transformOutputExpression(expr)

            val opos = pos
            pos += 1
            opos
          })

        indexes += idx

        expr
      }))

    (expressions ++ outputExpressions, indexes.toArray)
  }

  // count is not to be trusted, seems some funcs are evaluated twice
  def debugOutput[T](salienceArr: Array[Int], outArrTerm: Array[T], count: Int): GenericArrayData = {
    val out = new ArrayBuffer[(Int, T)](count + 1)//-1 start so boost by one, may still be too high
    var i = 0
    for( idx <- 0 until salienceArr.length){
      if (outArrTerm(idx) != null) {
        out += (salienceArr(idx) -> outArrTerm(idx))
        i += 1
      }
    }
    new org.apache.spark.sql.catalyst.util.GenericArrayData(
      out.sortBy(_._1).map( p => InternalRow(p._1, p._2) )
      )
  }

  def flattenSalience(ruleSuite: RuleSuite): Array[Int] =
    ruleSuite.ruleSets.flatMap( ruleSet => ruleSet.rules.map(rule =>
      rule.runOnPassProcessor match {
        case NoOpRunOnPassProcessor.noOp => qualityException(s"You cannot use a RuleEngineRunner if any of the rules do not have RunOnPassProcessors set ruleSet ${ruleSet.id}, rule ${rule.id}}")
        case r: RunOnPassProcessor => r.salience
      }
    )).toArray

  def flattenEngineIds(ruleSuite: RuleSuite): Array[(Long, Long, Long)] = //Array[(java.lang.Long, java.lang.Long, java.lang.Long)] =
    ruleSuite.ruleSets.flatMap( ruleSet => ruleSet.rules.map(rule =>
      rule.runOnPassProcessor match {
        case NoOpRunOnPassProcessor.noOp => qualityException(s"You cannot use a RuleEngineRunner if any of the rules do not have RunOnPassProcessors set ruleSet ${ruleSet.id}, rule ${rule.id}}")
        case r: RunOnPassProcessor => (packTheId(ruleSuite.id), packTheId(ruleSet.id), packTheId(rule.id))
      }
    )).toArray

  def reincorporateExpressions(ruleSuite: RuleSuite, expr: Seq[Expression], compileEvals: Boolean, expressionOffsets: Array[Int]): RuleSuite =
    reincorporateExpressionsF(ruleSuite, expr, (expr: Expression) => ExpressionWrapper(expr, compileEvals), (e: Expression)=>e, compileEvals, expressionOffsets)

  def reincorporateExpressionsF[T](ruleSuite: RuleSuite, expr: Seq[T], f: T => RuleLogic, processorExpression: T => Expression, compileEvals: Boolean, expressionOffsets: Array[Int]): RuleSuite = {
    val offset = expressionOffsets.length
    val itr = expr.zipWithIndex.iterator
    ruleSuite.copy(ruleSets = ruleSuite.ruleSets.map(
      ruleSet =>
        ruleSet.copy( rules = ruleSet.rules.map(
          rule => {
            val (nexpr, index) = itr.next()
            val outexpr = expr(offset + expressionOffsets(index))
            rule.copy(expression = f(nexpr), runOnPassProcessor =
              rule.runOnPassProcessor.withExpr(OutputExpressionWrapper(processorExpression(outexpr), compileEvals)))
          }
        ))
    ))
  }

  def compiledEvalDebug[T](results: InternalRow, output: T): InternalRow =
    InternalRow(results, null, output)

  def compiledEval[T](results: InternalRow, currentSalience: Int, rules: Array[(Long, Long, Long)], currentOutputIndex: Int, output: Array[T]): InternalRow =
    InternalRow(results,
        if (currentSalience == java.lang.Integer.MAX_VALUE)
          null
        else {
          val rule = rules(currentOutputIndex)
          InternalRow(rule._1, rule._2, rule._3)
        },
        if (currentSalience == java.lang.Integer.MAX_VALUE)
          null
        else output(currentOutputIndex)
      )

  case class CompilerTerms(funNames: _root_.scala.collection.Iterator[_root_.scala.Predef.String],
                           paramsCall: String, utilsName: String, ruleSuitTerm: String, ruleSuiteArrays: String, resArrTerm: String,
                           currentSalience: String, ruleTupleArrTerm: String, currentOutputIndex: String, outArrTerm: String,
                           salienceArrTerm: String)

  def genCompilerTerms[T: ClassTag](ctx:  _root_.org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext,
                  child: Expression, expressionOffsets: Array[Int], realChildren: Seq[Expression],
                       debugMode: Boolean, variablesPerFunc: Int, variableFuncGroup: Int, forceTriggerEval: Boolean,
                       extraResult: String => String = (_ : String) => "",
                       extraSetup: String => String = (_ : String) => "",
                       orderOffset: Int => Int = identity,
                       salienceCheck: Boolean = true
                      ):
    Option[CompilerTerms] = {
    val i = ctx.INPUT_ROW

    if (child.isInstanceOf[NonPassThrough] && (i eq null) ) {
      // for some reason code gen ends up with assuming iterator based gen on children instead of simple gen - the actual gen isn't even called for flattenResultsTest, only for withResolve, could be dragons.
      return None
    }

    val (paramsDef, paramsCall) =
      if (i ne null)
        (s"InternalRow $i", s"$i")
      else
        (ctx.currentVars.map(v => s"${if (v.value.javaType.isPrimitive) v.value.javaType else v.value.javaType.getName} ${v.value}, ${v.isNull.javaType} ${v.isNull}").mkString(", "),
          ctx.currentVars.map(v => s"${v.value}, ${v.isNull}").mkString(", "))

    // bind the rules
    val (ruleSuitTerm, termFun) = genRuleSuiteTerm[T](ctx)
    val utilsName = "com.sparkutils.quality.impl.RuleRunnerUtils"

    val childrenFuncTerm = termFun("compiledRealChildren", classOf[ExpressionWrapper].getName + "[]")

    val ruleSuiteArrays = ctx.addMutableState(classOf[RuleSuiteResultArray].getName,
      ctx.freshName("ruleSuiteArrays"),
      v => s"$v = $utilsName.ruleSuiteArrays($ruleSuitTerm);"
    )

    val currentSalience = ctx.addMutableState("int", ctx.freshName("currentSalience"),
      v => s"$v = java.lang.Integer.MAX_VALUE;"
    )
    val currentOutputIndex = ctx.addMutableState("int", ctx.freshName("currentOutputIndex"),
      v => s"$v = -1;"
    )

    val offset = expressionOffsets.size

    val ruleRes = "java.lang.Object"
    val resArrTerm = ctx.addMutableState(ruleRes+"[]", ctx.freshName("results"),
      v => s"$v = new $ruleRes[$offset];")

    val currRuleRes = "int"
    val currRuleResTerm = ctx.addMutableState(currRuleRes, ctx.freshName("currRuleRes"),
      v => s"$v = 0;")


    val ruleTupleRes = classOf[Tuple3[_,_,_]].getName
    val ruleTupleArrTerm = ctx.addMutableState(ruleTupleRes+"[]", ctx.freshName("ruleId"),
      v => s"$v = com.sparkutils.quality.impl.RuleEngineRunnerUtils.flattenEngineIds($ruleSuitTerm);")

    val salienceType = "int"
    val salienceArrTerm = ctx.addMutableState(salienceType+"[]", ctx.freshName("salience"),
      v => s"$v = com.sparkutils.quality.impl.RuleEngineRunnerUtils.flattenSalience($ruleSuitTerm);")

    val output = {
      val javaType = realChildren.last.genCode(ctx).value.javaType // last should always be good
      if (javaType.isPrimitive) javaType else javaType.getName
    }
    val outArrTerm = ctx.addMutableState(output+"[]", ctx.freshName("output"),
      v => s"$v = new $output[$offset];")

    val triggerRules = realChildren.slice(0, offset)

    def codeGen(exp: Expression, idx: Int, funName: String) = {
      val (evalPre, eval) =
        if (forceTriggerEval)
          ("", s"$utilsName.ruleResultToInt($childrenFuncTerm[$idx].eval($i))")
        else {
          val eval = exp.genCode(ctx)
          (eval.code, s"com.sparkutils.quality.RuleLogicUtils.anyToRuleResultInt(${eval.isNull} ? null : ${eval.value})")
        }

      val converted =
        s"""
            $evalPre
            $currRuleResTerm = $eval;

            $resArrTerm[$idx] = $currRuleResTerm;
            if ( ( $currRuleResTerm == $PassedInt ) ${if (!debugMode && salienceCheck) s" && ( $currentSalience > $salienceArrTerm[$idx] ) " else "" }) {
              $funName($paramsCall, $idx);
            }
            """

      converted
    }

    val index = ctx.freshName(s"triggerIndex")

    val outExprFunTerms =
      for{ i <- 0 until (realChildren.size - offset) } yield {

        val exprFuncName = ctx.freshName(s"outputExprFun$i")

        val exp = realChildren(offset + i)
        val eval = exp.genCode(ctx)

        ctx.addNewFunction(exprFuncName,
          s"""
   private void $exprFuncName($paramsDef, int $index) {
     ${
            if (debugMode)
              s"""
            ${extraSetup(index)} \n
            ${eval.code} \n

            $currentOutputIndex += 1; \n

            $outArrTerm[$index] = ${eval.isNull} ? null : ($output)${eval.value}; \n
            ${extraResult(s"$outArrTerm[$index]")}
            """
            else
              s"""
            ${extraSetup(index)} \n
            ${eval.code} \n

            $currentSalience = $salienceArrTerm[$index]; \n
            $currentOutputIndex = $index; \n

            $outArrTerm[$index] = ${eval.isNull} ? null : ($output)${eval.value}; \n
            ${extraResult(s"$outArrTerm[$index]")}
            """
          }
   }
  """
        )
      }

    // ensure ordering and re-use
    val allExpr = triggerRules.zipWithIndex.map { case (_, idx) =>

      val realI = orderOffset(idx)

      val offset = expressionOffsets(realI)
      val funName = outExprFunTerms(offset)
      val trigger = triggerRules(realI) // the original trigger is useless
      val stepWithIf = codeGen(trigger, realI, funName)

      stepWithIf
    }.grouped(variablesPerFunc).grouped(variableFuncGroup)

    Some(
      CompilerTerms(RuleRunnerUtils.generateFunctionGroups(ctx, allExpr, paramsDef, paramsCall),
        paramsCall, utilsName, ruleSuitTerm, ruleSuiteArrays, resArrTerm,
        currentSalience, ruleTupleArrTerm, currentOutputIndex, outArrTerm,
        salienceArrTerm)
    )
  }

}

/**
  * Children will be rewritten by the plan, it's then re-incorporated into ruleSuite
  * expressionOffsets.length is the length of the trigger expressions in realChildren, realChildren(expressionOffsets.length + expressionOffsets(x)) will be the correct OutputExpression
  */
case class RuleEngineRunner(ruleSuite: RuleSuite, child: Expression, resultDataType: DataType,
                            compileEvals: Boolean, debugMode: Boolean, variablesPerFunc: Int,
                            variableFuncGroup: Int, forceRunnerEval: Boolean, expressionOffsets: Array[Int],
                            forceTriggerEval: Boolean) extends UnaryExpression with NonSQLExpression with CodegenFallback {

  import RuleEngineRunnerUtils._

  lazy val realChildren =
    child match {
      case r @ NonPassThrough(_) => r.rules
      case PassThrough(children) => children
    }

  // only used for compilation
  lazy val compiledRealChildren = realChildren.slice(0, expressionOffsets.length).map(ExpressionWrapper(_, compileEvals)).toArray

  override def nullable: Boolean = false
  override def toString: String = s"RuleEngineRunner(${realChildren.mkString(", ")})"

  // used only for eval, compiled uses the children directly
  lazy val reincorporated = reincorporateExpressions(ruleSuite, realChildren, compileEvals, expressionOffsets)

  // keep it simple for this one. - can return an internal row or whatever..
  override def eval(input: InternalRow): Any = {
    val (res, rule, processedRes) = reincorporated.evalWithProcessors(input, debugMode)
    InternalRow(com.sparkutils.quality.impl.RuleRunnerUtils.ruleResultToRow(res),
      if (rule eq null) null else
      InternalRow(packId(rule._1),packId(rule._2),packId(rule._3)), processedRes)
  }

  def dataType: DataType = StructType( Seq(
      StructField(name = "ruleSuiteResults", dataType = com.sparkutils.quality.ruleSuiteResultType),
      StructField(name = "salientRule", dataType = com.sparkutils.quality.fullRuleIdType, nullable = true),
      StructField(name = "result", dataType = resultDataType, nullable = true)
    ))

  override protected def doGenCode(ctx:  _root_.org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext, ev:  _root_.org.apache.spark.sql.catalyst.expressions.codegen.ExprCode): _root_.org.apache.spark.sql.catalyst.expressions.codegen.ExprCode = {
    if (forceRunnerEval) {
      return super[CodegenFallback].doGenCode(ctx, ev)
    }

    ctx.references += this

    val compilerTerms =
      RuleEngineRunnerUtils.genCompilerTerms[RuleEngineRunner](ctx, child, expressionOffsets, realChildren,
        debugMode, variablesPerFunc, variableFuncGroup, forceTriggerEval).getOrElse(return super[CodegenFallback].doGenCode(ctx, ev))

    import compilerTerms._

    // for debug currentOutputIndex is the count of matches

    val pre = s"""
          $currentSalience = java.lang.Integer.MAX_VALUE;
          $currentOutputIndex = -1;
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
            com.sparkutils.quality.impl.RuleEngineRunnerUtils.compiledEvalDebug(
              $utilsName.evalArray($ruleSuitTerm, $ruleSuiteArrays, $resArrTerm),
            ($currentOutputIndex < 0) ? null : com.sparkutils.quality.impl.RuleEngineRunnerUtils.debugOutput($salienceArrTerm, $outArrTerm, $currentOutputIndex));

          $post
          """
        )
      else
        ev.copy(code = code"""
          $pre

          InternalRow ${ev.value} =
            com.sparkutils.quality.impl.RuleEngineRunnerUtils.compiledEval(
              $utilsName.evalArray($ruleSuitTerm, $ruleSuiteArrays, $resArrTerm),
              $currentSalience, $ruleTupleArrTerm, $currentOutputIndex, $outArrTerm);

          $post
          """
        )

    res

  }

  protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)
}
