package com.sparkutils.quality.impl

import com.sparkutils.quality.impl.RuleLogicUtils.mapRules
import com.sparkutils.quality.impl.RuleRunnerUtils.flattenExpressions
import com.sparkutils.quality.impl.imports.RuleResultsImports.packId
import com.sparkutils.quality._
import types.ruleSuiteResultType
import com.sparkutils.quality.impl.imports.RuleRunnerImports
import com.sparkutils.quality.impl.util.{NonPassThrough, PassThrough}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression, UnaryExpression}
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, DataFrame, QualitySparkUtils}

import scala.reflect.ClassTag

object PackId {

  def packId(anyId: Any) = {
    val r = anyId.asInstanceOf[Id]
    ((r.id.toLong) << 32) | (r.version & 0xffffffffL)
  }

  def unpack(a: Any): Id =
    if (a == null)
      a.asInstanceOf[Id]
    else
      unpack(a.asInstanceOf[Long])

  def unpack(a: Long) = {
    val id = a >> 32
    val version = a.toInt
    Id(id.toInt, version) // lookup goes here
  }
}

protected[quality] object RuleRunnerImpl {


  /**
   * Creates a column that runs the RuleSuite.  This also forces registering the lambda functions used by that RuleSuite
   *
   * @param ruleSuite The Qualty RuleSuite to evaluate
   * @param compileEvals Should the rules be compiled out to interim objects - by default true for eval usage, wholeStageCodeGen will evaluate in place
   * @param resolveWith This experimental parameter can take the DataFrame these rules will be added to and pre-resolve and optimise the sql expressions, see the documentation for details on when to and not to use this. RuleRunner does not currently do wholestagecodegen when resolveWith is used.
   * @param variablesPerFunc Defaulting to 40, it allows, in combination with variableFuncGroup customisation of handling the 64k jvm method size limitation when performing WholeStageCodeGen.  You _shouldn't_ need it but it's there just in case.
   * @param variableFuncGroup Defaulting to 20
   * @param forceRunnerEval Defaulting to false, passing true forces a simplified partially interpreted evaluation (compileEvals must be false to get fully interpreted)
   * @return A Column representing the Quality DQ expression built from this ruleSuite
   */
  def ruleRunnerImpl(ruleSuite: RuleSuite, compileEvals: Boolean = true, resolveWith: Option[DataFrame] = None, variablesPerFunc: Int = 40, variableFuncGroup: Int = 20, forceRunnerEval: Boolean = false): Column = {
    com.sparkutils.quality.registerLambdaFunctions( ruleSuite.lambdaFunctions )
    val flattened = flattenExpressions(ruleSuite)
    val runner = new RuleRunner(RuleLogicUtils.cleanExprs(ruleSuite), PassThrough(flattened), compileEvals, variablesPerFunc, variableFuncGroup, forceRunnerEval)
    new Column(
      QualitySparkUtils.resolveWithOverride(resolveWith).map { df =>
        val resolved = QualitySparkUtils.resolveExpression(df, runner)

        resolved.asInstanceOf[RuleRunner].copy(child = resolved.children(0) match {
          // replace the expr
          case PassThrough(children) => NonPassThrough(children)
        })
      } getOrElse runner
    )
  }

}

private[quality] object RuleRunnerUtils extends RuleRunnerImports {

  def ruleResultToInt(ruleResult: RuleResult): Int =
    ruleResult match {
      case Failed => FailedInt
      case SoftFailed => SoftFailedInt
      case DisabledRule => DisabledRuleInt
      case Passed => PassedInt
      case Probability(percentage) => (percentage * PassedInt).toInt
      case RuleResultWithProcessor(res, _) => ruleResultToInt(res)
    }

  def flattenExpressions(ruleSuite: RuleSuite): Seq[Expression] =
    ruleSuite.ruleSets.flatMap(ruleSet => ruleSet.rules.map(rule =>
      rule.expression match {
        case r: ExprLogic => r.expr // only ExprLogic are possible here
      }))

  def reincorporateExpressions(ruleSuite: RuleSuite, expr: Seq[Expression], compileEvals: Boolean = true): RuleSuite =
    reincorporateExpressionsF(ruleSuite, expr, (expr: Expression) => ExpressionWrapper(expr, compileEvals))

  def reincorporateExpressionsF[T](ruleSuite: RuleSuite, expr: Seq[T], f: T => RuleLogic): RuleSuite = {
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

  def evalArray(ruleSuite: RuleSuite, ruleSuiteArrays: RuleSuiteResultArray, results: Array[Any]): InternalRow = {
    import ruleSuite._

    val ruleSetRes = Array.ofDim[InternalRow](ruleSuiteArrays.ruleSetIds.length)

    var rsOverall = PassedInt

    var offset = 0

    for( rsi <- 0 until ruleSuiteArrays.ruleSetIds.length) {

      val rulesetSize = ruleSuiteArrays.ruleSets(rsi).length

      val ruleSetResults = results.slice(offset, offset + rulesetSize)
      offset += rulesetSize

      val overall = ruleSetResults.foldLeft(PassedInt){
        (ov, res) =>
          OverallResult.inplaceInt(ov, res.asInstanceOf[Int], probablePass) // convert needed for process
      }

      rsOverall = OverallResult.inplaceInt(overall, rsOverall, probablePass)

      ruleSetRes(rsi) = InternalRow(
        overall: java.lang.Integer,
        ArrayBasedMapData( ruleSuiteArrays.ruleSets(rsi), ruleSetResults )
        )
    }

    InternalRow( ruleSuiteArrays.packedId,
      rsOverall: java.lang.Integer,
      ArrayBasedMapData( ruleSuiteArrays.ruleSetIds, ruleSetRes)
    )
  }

  def ruleResultToRow(ruleSuiteResult: RuleSuiteResult): InternalRow =
    InternalRow(
      packId(ruleSuiteResult.id),
      ruleResultToInt(ruleSuiteResult.overallResult),
      ArrayBasedMapData(
        ruleSuiteResult.ruleSetResults, packId, (a: Any) => {
          val v = a.asInstanceOf[RuleSetResult]
          InternalRow(
            ruleResultToInt(v.overallResult),
            ArrayBasedMapData(
              v.ruleResults, packId, (a: Any) => ruleResultToInt(a.asInstanceOf[RuleResult])
            )
          )
        }
      )
    )

  def packTheId(obj: Object) = packId(obj)//: java.lang.Long

  protected[quality] def generateFunctionGroups(ctx: CodegenContext, allExpr: Iterator[Seq[String]]#GroupedIterator[Seq[String]], paramsDef: String, paramsCall: String) = {
    val funNames =
      for (exprGroup <- allExpr) yield {
        val groupName = ctx.freshName("ruleRunnerEGroup")
        ctx.addNewFunction(groupName, {
          val funNames =
            for {
              exprFunc <- exprGroup
            } yield {
              val exprFuncName = ctx.freshName("ruleRunnerEFuncGroup")
              ctx.addNewFunction(exprFuncName,
s"""
   private void $exprFuncName($paramsDef) {
     ${exprFunc.mkString("\n")}
   }
  """
              )
            }

s"""
   private void $groupName($paramsDef) {
     ${funNames.map { f => s"$f($paramsCall);" }.mkString("\n")}
   }
   """

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
case class RuleRunner(ruleSuite: RuleSuite, child: Expression, compileEvals: Boolean,
                      variablesPerFunc: Int, variableFuncGroup: Int, forceRunnerEval: Boolean) extends UnaryExpression with NonSQLExpression with CodegenFallback {

  import RuleRunnerUtils._

  lazy val realChildren =
    child match {
      case r @ NonPassThrough(_) => r.rules
      case PassThrough(children) => children
    }

  override def nullable: Boolean = false
  override def toString: String = s"RuleRunner(${realChildren.mkString(", ")})"

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
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    if (forceRunnerEval) {
      return super[CodegenFallback].doGenCode(ctx, ev)
    }
    val i = ctx.INPUT_ROW

    if (child.isInstanceOf[NonPassThrough] && (i eq null) ) {
      // for some reason code gen ends up with assuming iterator based gen on children instead of simple gen - the actual gen isn't even called for flattenResultsTest, only for withResolve, could be dragons.
      return super[CodegenFallback].doGenCode(ctx, ev)
    }

    ctx.references += this

    // bind the rules
    val ruleSuitTerm = genRuleSuiteTerm[RuleRunner](ctx)._1
    val utilsName = "com.sparkutils.quality.impl.RuleRunnerUtils"

    val ruleSuiteArrays = ctx.addMutableState(classOf[RuleSuiteResultArray].getName,
      ctx.freshName("ruleSuiteArrays"),
      v => s"$v = $utilsName.ruleSuiteArrays($ruleSuitTerm);"
    )

    val ruleRes = "java.lang.Object"
    val arrTerm = ctx.addMutableState(ruleRes+"[]", ctx.freshName("results"),
      v => s"$v = new $ruleRes[${realChildren.size}];")

    val allExpr = realChildren.zipWithIndex.map { case (child, idx) =>
        val eval = child.genCode(ctx)
        val converted =
          s"""${eval.code}\n

             $arrTerm[$idx] = ${eval.isNull} ? null : com.sparkutils.quality.impl.RuleLogicUtils.anyToRuleResultInt(${eval.value});"""

        converted
    }.grouped(variablesPerFunc).grouped(variableFuncGroup)

    val (paramsDef, paramsCall) =
      if (i ne null)
        (s"InternalRow $i", s"$i")
      else
        (ctx.currentVars.map(v => s"${if (v.value.javaType.isPrimitive) v.value.javaType else v.value.javaType.getName} ${v.value}, ${v.isNull.javaType} ${v.isNull}").mkString(", "), ctx.currentVars.map(v => s"${v.value}, ${v.isNull}").mkString(", "))

    val funNames: _root_.scala.collection.Iterator[_root_.scala.Predef.String] =
      RuleRunnerUtils.generateFunctionGroups(ctx, allExpr, paramsDef, paramsCall)

    val res = ev.copy(code = code"""
      ${funNames.map{f => s"$f($paramsCall);"}.mkString("\n")}

      InternalRow ${ev.value} = $utilsName.evalArray($ruleSuitTerm, $ruleSuiteArrays, $arrTerm);
      boolean ${ev.isNull} = false;
      """
    )

    res

  }

  protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)
}
