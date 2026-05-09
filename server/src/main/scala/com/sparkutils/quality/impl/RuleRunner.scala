package com.sparkutils.quality.impl

import com.sparkutils.quality
import com.sparkutils.quality.RuleSuite.mapRules
import com.sparkutils.quality.impl.RuleRunnerUtils.{flattenExpressions, ruleSuiteArrays}
import com.sparkutils.quality.impl.PackId.packId
import com.sparkutils.quality._
import com.sparkutils.quality.impl.ExpressionRuleExpr.ExpressionRuleOps
import com.sparkutils.quality.impl.GetRealChildren.getRealChildren
import types.ruleSuiteResultType
import com.sparkutils.quality.impl.imports.RuleRunnerImports
import com.sparkutils.quality.impl.util.Serializing.ruleResultToInt
import com.sparkutils.quality.impl.util.{NonPassThrough, PassThroughCompileEvals}
import org.apache.spark.sql.ClassicQualitySparkUtils.genParams
import org.apache.spark.sql.ShimUtils.column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.codegen.{Block, CodegenContext, CodegenFallback, ExprCode, ExprValue}
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, truncatedString}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{ClassicQualitySparkUtils, Column, DataFrame, ShimUtils}

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
                     variablesPerFunc: Int = 40, variableFuncGroup: Int = 20, forceRunnerEval: Boolean = false): Column = {
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
          compileEvals, variablesPerFunc, variableFuncGroup)
      else
        new RuleRunner(cleaned, input,
          compileEvals, variablesPerFunc, variableFuncGroup)

    column(
      ClassicQualitySparkUtils.resolveWithOverride(resolveWith).map { df =>
        val resolved = ClassicQualitySparkUtils.resolveExpression(df, runner)

        resolved.withNewChildren(resolved.children.map{
          // replace the expr
          case PassThroughCompileEvals(child) => NonPassThrough(child)
          case child => NonPassThrough(child)
        })
      } getOrElse runner
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

  case class RuleSuiteResultArray(packedId: Long, ruleSetIds: Array[Long], ruleSets: Array[Array[Long]]) {

  }

  // create base arrays for each maps
  def ruleSuiteArrays(ruleSuite: RuleSuite): RuleSuiteResultArray = {
    val (ruleSetIds, rulesArrays) =
      ruleSuite.ruleSets.map{
        ruleSet =>
          packTheId(ruleSet.id) ->
            ruleSet.rules.map(r => packTheId(r.id)).toArray

      }.unzip // make sure no funkyness on ordering occurs

    RuleSuiteResultArray(packTheId(ruleSuite.id),
      ruleSetIds.toArray, rulesArrays.toArray)
  }

  def evalArray(ruleSuite: RuleSuite, ruleSuiteArrays: RuleSuiteResultArray, results: Array[Any], startingResult: Int, processOverall: (Int, Int, Double) => Int): InternalRow = {
    import ruleSuite._

    val ruleSetRes = Array.ofDim[InternalRow](ruleSuiteArrays.ruleSetIds.length)

    var rsOverall = startingResult

    var offset = 0

    for (rsi <- 0 until ruleSuiteArrays.ruleSetIds.length) {

      val rulesetSize = ruleSuiteArrays.ruleSets(rsi).length

      val ruleSetResults = results.slice(offset, offset + rulesetSize)
      offset += rulesetSize

      val overall = ruleSetResults.foldLeft(startingResult) {
        (ov, res) =>
          processOverall(res.asInstanceOf[Int], ov, probablePass) // convert needed for process
      }

      rsOverall = processOverall(overall, rsOverall, probablePass)

      ruleSetRes(rsi) = InternalRow(
        overall: java.lang.Integer,
        ArrayBasedMapData(ruleSuiteArrays.ruleSets(rsi), ruleSetResults)
      )
    }

    InternalRow(ruleSuiteArrays.packedId,
      rsOverall: java.lang.Integer,
      ArrayBasedMapData(ruleSuiteArrays.ruleSetIds, ruleSetRes)
    )
  }

  def evalArray(ruleSuite: RuleSuite, ruleSuiteArrays: RuleSuiteResultArray, results: Array[Any]): InternalRow =
    evalArray(ruleSuite, ruleSuiteArrays, results, PassedInt, OverallResultHelper.inplaceInt)

  def evalArrayForDefault(ruleSuite: RuleSuite, ruleSuiteArrays: RuleSuiteResultArray, results: Array[Any]): InternalRow =
    evalArray(ruleSuite, ruleSuiteArrays, results, FailedInt, OverallResultHelper.inplaceForDefaultInt)

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

  protected[quality] def generateFunctionGroups(ctx: CodegenContext, allExpr: Iterator[Seq[Block]]#GroupedIterator[Seq[Block]],
    paramsDef: String, paramsCall: String, prefix: String = "ruleRunner", exprEnd: () => Block = () => code"",
                                                exprFunEnd: () => Block = () => code"") = {
    val funNames =
      for (exprGroup <- allExpr) yield {
        val groupName = ctx.freshName(prefix+"EGroup")
        ctx.addNewFunction(groupName, {
          val funNames =
            for {
              exprFunc <- exprGroup
            } yield {
              val exprFuncName = ctx.freshName(prefix+"EFuncGroup")
              ctx.addNewFunction(exprFuncName,
code"""
   private void $exprFuncName($paramsDef) {
     ${exprFunc.mkString(s"${exprEnd()}\n")}
   }
  """.code
              )
            }

code"""
   private void $groupName($paramsDef) {
     ${funNames.map { f => s"$f($paramsCall);" }.mkString(s"${exprFunEnd()}\n")}
   }
   """.code

        })
      }
    funNames
  }

  def genRuleSuiteTerm[T: ClassTag](ctx: CodegenContext): (String, (String, String) => String) = {
    val ruleSuiteClassName = classOf[RuleSuite].getName
    val ruleRunnerClassName = implicitly[ClassTag[T]].runtimeClass.getName
    val ruleRunnerExpressionIdx = ctx.references.size - 1
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

  def nonOutputRuleGen(ctx: CodegenContext, runner: Expression, ev: ExprCode, ruleSuitTerm: String, utilsName: String,
                       realChildren: Seq[Expression], variablesPerFunc: Int, variableFuncGroup: Int,
                       resultF: (ExprValue, Int) => String
                      ): ExprCode = {
    val ruleSuiteArrays = ctx.addMutableState(classOf[RuleSuiteResultArray].getName,
      ctx.freshName("ruleSuiteArrays"),
      v => s"$v = com.sparkutils.quality.impl.RuleRunnerUtils.ruleSuiteArrays($ruleSuitTerm);"
    )

    val paramInfo = genParams(ctx, runner)
    import paramInfo._

    val ruleRes = "java.lang.Object"
    val arrTerm = ctx.addMutableState(ruleRes + "[]", ctx.freshName("results"),
      v => s"$v = new $ruleRes[${realChildren.size}];")

    val allExpr = realChildren.zipWithIndex.map { case (child, idx) =>
      val eval = child.genCode(ctx)

      val converted =
        code"""${eval.code}\n

             $arrTerm[$idx] = ${eval.isNull} ? null : ${resultF(eval.value, idx)};"""

      converted
    }.grouped(variablesPerFunc).grouped(variableFuncGroup)

    val funNames: Iterator[String] =
      RuleRunnerUtils.generateFunctionGroups(ctx, allExpr, paramsDef, paramsCall)

    val res = ev.copy(code =
      code"""
      $pushToTop
      ${funNames.map { f => s"$f($paramsCall);" }.mkString("\n")}

      InternalRow ${ev.value} = $utilsName.evalArray($ruleSuitTerm, $ruleSuiteArrays, $arrTerm);
      boolean ${ev.isNull} = false;
      """
    )

    res
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
trait RuleRunnerBase[T] extends NonSQLExpression {

  val ruleSuite: RuleSuite
  val compileEvals: Boolean
  val variablesPerFunc: Int
  val variableFuncGroup: Int

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
  protected def doGenCodeI(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    ctx.references += this

    // bind the rules
    val ruleSuitTerm = genRuleSuiteTerm[T](ctx)._1
    val utilsName = "com.sparkutils.quality.impl.RuleRunnerUtils"

    nonOutputRuleGen(ctx, this, ev, ruleSuitTerm, utilsName, realChildren, variablesPerFunc, variableFuncGroup,
      (code: ExprValue, idx: Int) => s"com.sparkutils.quality.impl.RuleLogicUtils.anyToRuleResultInt($code)"
    )
  }
}

case class RuleRunnerEval(ruleSuite: RuleSuite, children: Seq[Expression], compileEvals: Boolean,
                      variablesPerFunc: Int, variableFuncGroup: Int)
  extends RuleRunnerBase[RuleRunnerEval] with CodegenFallback {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)

  override implicit val tClass: ClassTag[RuleRunnerEval] = ClassTag(classOf[RuleRunnerEval])
}

case class RuleRunner(ruleSuite: RuleSuite, children: Seq[Expression], compileEvals: Boolean,
                          variablesPerFunc: Int, variableFuncGroup: Int)
  extends RuleRunnerBase[RuleRunner] {

  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = doGenCodeI(ctx, ev)

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)

  override implicit val tClass: ClassTag[RuleRunner] = ClassTag(classOf[RuleRunner])
}
