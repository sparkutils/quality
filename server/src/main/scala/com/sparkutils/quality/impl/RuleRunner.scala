package com.sparkutils.quality.impl

import com.sparkutils.quality
import com.sparkutils.quality.RuleSuite.mapRules
import com.sparkutils.quality.impl.RuleRunnerUtils.flattenExpressions
import com.sparkutils.quality.impl.PackId.packId
import com.sparkutils.quality._
import com.sparkutils.quality.impl.ExpressionRuleExpr.ExpressionRuleOps
import com.sparkutils.quality.impl.GetRealChildren.getRealChildren
import com.sparkutils.quality.impl.extension.ZeroCodeGenWrap
import types.ruleSuiteResultType
import com.sparkutils.quality.impl.imports.RuleRunnerImports
import com.sparkutils.quality.impl.util.ExtraConfig.ConfigMapOps
import com.sparkutils.quality.impl.util.Serializing.ruleResultToInt
import com.sparkutils.quality.impl.util.{GenerateResult, NonPassThrough, ParameterInformation, PassThroughCompileEvals, SeparateCompilation}
import org.apache.spark.sql.ClassicQualitySparkUtils.genParams
import org.apache.spark.sql.ShimUtils.column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.codegen.JavaCode.isNullVariable
import org.apache.spark.sql.catalyst.expressions.codegen.{Block, CodegenContext, CodegenFallback, ExprCode, ExprValue, VariableValue}
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, truncatedString}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{ClassicQualitySparkUtils, Column, DataFrame, ShimUtils}

import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

protected[quality] object RuleRunnerImpl {

  /**
   * Creates a column that runs the RuleSuite.  This also forces registering the lambda functions used by that RuleSuite
   *
   * @param ruleSuite The Qualty RuleSuite to evaluate
   * @param compileEvals Should the rules be compiled out to interim objects - by default false, allowing optimisations
   * @param resolveWith This experimental parameter can take the DataFrame these rules will be added to and pre-resolve and optimise the sql expressions, see the documentation for details on when to and not to use this. RuleRunner does not currently do wholestagecodegen when resolveWith is used.
   * @param variablesPerFunc Defaulting to 40, it allows, in combination with variableFuncGroup customisation of handling the 64k jvm method size limitation when performing WholeStageCodeGen.  You _shouldn't_ need it but it's there just in case.
   * @param variableFuncGroup Defaulting to 20
   * @param forceRunnerEval Defaulting to false, passing true forces a simplified partially interpreted evaluation (compileEvals must be false to get fully interpreted)
   * @return A Column representing the Quality DQ expression built from this ruleSuite
   */
  def ruleRunnerImplClassic(ruleSuite: RuleSuite, compileEvals: Boolean = false, resolveWith: Option[DataFrame] = None,
                     variablesPerFunc: Int = 40, variableFuncGroup: Int = 20, forceRunnerEval: Boolean = false,
                     extraConfig: Map[String, String] = Map.empty): Column = {
    com.sparkutils.quality.registerLambdaFunctions( ruleSuite.lambdaFunctions )
    val flattened = flattenExpressions(ruleSuite)
    val cleaned = RuleLogicUtils.cleanExprs(ruleSuite)
    val input =
      // ExpressionProxy and SubExprEvaluationRuntime cannot be used with compileEvals
      if (compileEvals)
        flattened.map(PassThroughCompileEvals)
      else
        flattened

    val runner =
      if (forceRunnerEval || resolveWith.isDefined)
        new RuleRunnerEval(cleaned, input,
          compileEvals, variablesPerFunc, variableFuncGroup, extraConfig)
      else
        new RuleRunner(cleaned, input,
          compileEvals, variablesPerFunc, variableFuncGroup, extraConfig)

    column(
      ClassicQualitySparkUtils.resolveWithOverride(resolveWith).map { df =>
        val resolved = ClassicQualitySparkUtils.resolveExpression(df, runner)

        resolved.withNewChildren(resolved.children.map{
          // replace the expr
          case PassThroughCompileEvals(child) => NonPassThrough(child)
          case child => NonPassThrough(child)
        })
      } getOrElse ZeroCodeGenWrap.wrap(runner)
    )
  }

  /**
   * Creates a column that runs the RuleSuite.  This also forces registering the lambda functions used by that RuleSuite
   *
   * NOTE resolveWith, compileEvals and forceRunnerEval are ignored and use defaults, prefer using dqRuleRunner
   *
   * @param ruleSuite The Qualty RuleSuite to evaluate
   * @param compileEvals Should the rules be compiled out to interim objects - by default false, allowing optimisations
   * @param variablesPerFunc Defaulting to 40, it allows, in combination with variableFuncGroup customisation of handling the 64k jvm method size limitation when performing WholeStageCodeGen.  You _shouldn't_ need it but it's there just in case.
   * @param variableFuncGroup Defaulting to 20
   * @param forceRunnerEval Defaulting to false, passing true forces a simplified partially interpreted evaluation (compileEvals must be false to get fully interpreted)
   * @return A Column representing the Quality DQ expression built from this ruleSuite
   */
  @deprecated(since="0.2.0", message="Use dqRuleRunner instead")
  def ruleRunnerImpl(ruleSuite: RuleSuite, compileEvals: Boolean = false,
                     variablesPerFunc: Int = 40, variableFuncGroup: Int = 20, forceRunnerEval: Boolean = false): Column =
    Runners.ruleRunner(ruleSuite, variablesPerFunc = variablesPerFunc, variableFuncGroup = variableFuncGroup).getOrElse(
      ShimUtils.callFunction("dq_rule_runner", lit(RuleSuiteHelpers.serialize(ruleSuite)),
        lit(variablesPerFunc), lit(variableFuncGroup))
    )

}

private[quality] object RuleRunnerUtils extends RuleRunnerImports {

  def flattenExpressions(ruleSuite: RuleSuite): Seq[Expression] =
    ruleSuite.ruleSets.flatMap(ruleSet => ruleSet.rules.map(rule =>
      rule.expression match {
        case r: ExprLogic => r.expr// only ExprLogic are possible here
        case r: quality.ExpressionRule => r.toImpl.expr
      }))

  def reincorporateExpressions(ruleSuite: RuleSuite, expr: Seq[Expression], compileEvals: Boolean = true): RuleSuite =
    reincorporateExpressionsF(ruleSuite, expr, (expr: Expression) => ExpressionWrapper(expr, compileEvals))

  def reincorporateExpressionsF[T](ruleSuite: RuleSuite, expr: Seq[T], f: T => RuleLogic[_]): RuleSuite = {
    val itr = expr.iterator
    mapRules(ruleSuite) { rule =>
      rule.copy(expression = f(itr.next()))
    }
  }

  def ruleResultToRow(ruleSuiteResult: RuleSuiteResult): InternalRow =
    InternalRow(
      packId(ruleSuiteResult.id),
      ruleResultToInt(ruleSuiteResult.overallResult),
      ArrayBasedMapData(
        ruleSuiteResult.ruleSetResults, packId _, (a: Any) => {
          val v = a.asInstanceOf[RuleSetResult]
          InternalRow(
            ruleResultToInt(v.overallResult),
            ArrayBasedMapData(
              v.ruleResults, packId _, (a: Any) => ruleResultToInt(a.asInstanceOf[RuleResult])
            )
          )
        }
      )
    )

  def packTheId(obj: Object) = packId(obj)//: java.lang.Long

  protected[quality] def generateFunctionGroups(
    ctx: CodegenContext, runner: Runner, params: ParameterInformation, resultRow: String,
    additionalParams: Seq[(VariableValue, Boolean)],
    expressions: Seq[(Trigger, (CodegenContext, ParameterInformation, Expression, Boolean) => Block)],
    prefix: String = "ruleRunner", exprEnd: () => Block = () => code"",
    groupSalienceCheck: String => Block = _ => code"", returnIfGroupSalienceCheckFalse: Boolean = false): TriggerResult = {

    val impl: TriggerGrouper = Triggers.loadTriggerGrouper(runner.extraConfig)

    val start = System.nanoTime()

    val res =
      impl.apply(ctx, runner, resultRow, additionalParams, expressions, params,
        prefix, exprEnd, groupSalienceCheck, returnIfGroupSalienceCheckFalse)

    val end = System.nanoTime()
    val groupingTime = Duration.fromNanos(end - start)
    if (runner.extraConfig.boolean(showGroupingTime, false)){
      println(s"$prefix RuleSuite - took ${groupingTime.toMinutes}m${groupingTime.toSeconds % 60}s to group")
      System.out.flush()
    }

    res
  }

  def genRuleSuiteTerm[T: ClassTag](ctx: CodegenContext, ruleRunnerExpressionIdx: Int): (String, (String, String) => String) = {
    val ruleSuiteClassName = classOf[RuleSuite].getName
    val ruleRunnerClassName = implicitly[ClassTag[T]].runtimeClass.getName
    val ruleSuitTerm = ctx.addMutableState(ruleSuiteClassName, ctx.freshName("ruleSuite"),
      v => s"$v = ($ruleSuiteClassName)((($ruleRunnerClassName)references" +
        s"[$ruleRunnerExpressionIdx]).ruleSuite());")

    val realChildrenTerm =
      (funname: String, className: String) =>
        ctx.addMutableState(className, ctx.freshName(funname),
      v => s"$v = ($className)((($ruleRunnerClassName)references" +
        s"[$ruleRunnerExpressionIdx]).$funname());")

    (ruleSuitTerm, realChildrenTerm)
  }

  def nonOutputRuleGen[T: ClassTag](ctx: CodegenContext, runner: Runner, ev: ExprCode, utilsName: String,
                       realChildren: Seq[Expression], resultF: (ExprValue, Int) => String,
                       ruleRunnerExpressionIdx: Int
                      ): (ExprCode, TriggerResult) = {
    val paramInfo = genParams(ctx, runner)
    import paramInfo._

    val resTerms = resultRowTerms(ctx, ruleRunnerExpressionIdx)
    import resTerms._

    val inPlaceOffsets = runner.inPlaceArrayOffsets(ctx, resultRow, ruleRunnerExpressionIdx)

    val allExpr = realChildren.zipWithIndex.map { case (child, idx) =>
      val generate =
        (ctx: CodegenContext, p: ParameterInformation, e: Expression, b: Boolean) => {
          val eval = e.genCode(ctx)

          code"""${eval.code}\n

            ${inPlaceOffsets.offsets(idx).apply(resultF(eval.value, idx))}
             """
        }
      (Trigger(child, idx, 0), generate)
    }

    val resName = ctx.freshName("result")
    val resNull = ctx.freshName("isNull")

    val groups = RuleRunnerUtils.generateFunctionGroups(ctx, runner, paramInfo, resultRow,
      Seq((VariableValue(resultRow, classOf[InternalRow]), false)), allExpr)

    val funNames: Iterator[String] = groups.groupCalls

    val exp = ExprCode(VariableValue(resName, ev.value.javaType), isNullVariable(resNull))

    val res = exp.copy(code =
      code"""
      // copy row
      $resultRowCopy
      ${funNames.map { f => s"$f($paramsCall);" }.mkString("\n")}

      InternalRow ${exp.value} = $resultRow;
      boolean ${exp.isNull} = false;
      """
    )

    (res, groups)
  }

  case class ResultRowTerms(runnerClassName: String, resultRow: String, resultRowCopy: String)

  protected[quality] def resultRowTerms[T: ClassTag](ctx: CodegenContext, ruleRunnerExpressionIdx: Int):
    ResultRowTerms = {
    val runnerClassName = implicitly[ClassTag[T]].runtimeClass.getName

    val original = ctx.addMutableState("InternalRow", "theOriginal", v =>
      s"$v = (($runnerClassName)references[$ruleRunnerExpressionIdx]).createDefaultRuleResult();")
    val resultRow = ctx.addMutableState("InternalRow", "resultRow", v => s"$v = null;")
    val resultRowCopy = s"$resultRow = $original.copy();"

    ResultRowTerms(runnerClassName, resultRow, resultRowCopy)
  }
}

/**
 * Children will be rewritten by the plan, either by resolveWith, it's then re-incorporated into ruleSuite
 *
 * The variablePerFunc (40) and variableFuncGroup (20) parameters are chosen due to 64k method limitation,
 * is null for all of a 1600 column row blasts the 64k jvm limit.  You may customise these values if a given rulesuite
 * requires it.
 *
 * @param variablesPerFunc How many variables are in a function
 * @param variableFuncGroup How many functions are then grouped into a new function
 */
trait RuleRunnerBase[T] extends NonSQLExpression with SplitCompilation with TriggerOnly {

  val compileEvals: Boolean

  implicit val tClass: ClassTag[T]

  import RuleRunnerUtils._

  lazy val realChildren = getRealChildren(children)

  override def nullable: Boolean = false
  override def toString: String = "RuleRunner" + truncatedString(
    realChildren, "(", ", ", ")", SQLConf.get.maxToStringFields)

  // used only for eval, compiled uses the children directly
  lazy val reincorporated = reincorporateExpressions(ruleSuite, realChildren, compileEvals)

  // keep it simple for this one. - can return an internal row or whatever..
  override def eval(input: InternalRow): Any = {
    val res = RuleSuiteFunctions.eval(reincorporated, input)
    ruleResultToRow(res)
  }

  def dataType: DataType = ruleSuiteResultType

  /**
   * Instead of evaluating through the RuleSuite structure the expressions are evaluated back to back, then
   * reincorporated via just the results afterwards before working out the overall results
   * @param ctx
   * @param ev
   * @return
   */
  protected def doGenCodeI(outerCtx: CodegenContext, ev: ExprCode): ExprCode = {

    val SeparateCompilation(clazz, fres, _) =
      SeparateCompilation.withSubExpressions(this,
        Triggers.loadTriggerGrouper(extraConfig).useChildrenForRunner(realChildren), outerCtx, ev, ruleSuite.id,
        topLevelCompilationUnit = true) {
      (ctx, ruleRunnerExpressionIdx, _) =>

        // must be called before the rule gen runs
        val params = genParams(ctx, this)

        // bind the rules
        val utilsName = "com.sparkutils.quality.impl.RuleRunnerUtils"

        val (res, triggerRes) =
          nonOutputRuleGen[T](ctx, this, ev, utilsName, realChildren,
            (code: ExprValue, idx: Int) => s"com.sparkutils.quality.impl.RuleLogicUtils.anyToRuleResultInt($code)",
            ruleRunnerExpressionIdx
          )

      GenerateResult((params, classOf[RuleRunnerBase[T]].getName), res, Seq.empty)
    }
    setClazzSource(clazz)
    fres
  }
}

case class RuleRunnerEval(ruleSuite: RuleSuite, children: Seq[Expression], compileEvals: Boolean,
                          variablesPerFunc: Int, variableFuncGroup: Int, extraConfig: Map[String, String],
                          alreadyZero: Boolean = false)
  extends RuleRunnerBase[RuleRunnerEval] with CodegenFallback {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)

  override implicit val tClass: ClassTag[RuleRunnerEval] = ClassTag(classOf[RuleRunnerEval])

  override def withZeroCode(): Runner = copy(alreadyZero = true)
}

case class RuleRunner(ruleSuite: RuleSuite, children: Seq[Expression], compileEvals: Boolean,
                      variablesPerFunc: Int, variableFuncGroup: Int, extraConfig: Map[String, String],
                      alreadyZero: Boolean = false)
  extends RuleRunnerBase[RuleRunner] {

  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = doGenCodeI(ctx, ev)

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)

  override implicit val tClass: ClassTag[RuleRunner] = ClassTag(classOf[RuleRunner])

  override def withZeroCode(): Runner = copy(alreadyZero = true)
}
