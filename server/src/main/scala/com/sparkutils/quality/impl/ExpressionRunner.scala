package com.sparkutils.quality.impl

import com.sparkutils.quality._
import com.sparkutils.quality.impl.GetRealChildren.getRealChildren
import com.sparkutils.quality.impl.RuleRunnerUtils.{flattenExpressions, genRuleSuiteTerm, nonOutputRuleGen, packTheId, reincorporateExpressions}
import com.sparkutils.quality.impl.PackId.packId
import com.sparkutils.quality.impl.extension.ZeroCodeGenWrap
import com.sparkutils.quality.impl.util.{Arrays, GenerateResult, PassThroughCompileEvals, SeparateCompilation}
import com.sparkutils.quality.impl.yaml.YamlEncoderExpr
import com.sparkutils.quality.impl.types._
import org.apache.spark.sql.ClassicQualitySparkUtils.genParams
import org.apache.spark.sql.{Column, ShimUtils}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode, ExprValue}
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression, UnaryExpression}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData, MapData, truncatedString}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.shim.expressions.InputTypeChecks
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import scala.reflect.ClassTag

object ExpressionRunner {
  /**
   * Runs the ruleSuite expressions saving results as a tuple of (ruleResult: yaml, resultType: String)
   * Supplying a ddlType triggers the output type for the expression to be that ddl type, rather than using yaml conversion.
   * @param renderOptions provides rendering options to the underlying snake yaml implementation
   * @param ddlType optional DDL string, when present yaml output is disabled and the output expressions must all have the same type
   * @param name the default column name "expressionResults"
   */
  def apply(ruleSuite: RuleSuite, name: String = "expressionResults", renderOptions: Map[String, String] = Map.empty,
            ddlType: String = "", variablesPerFunc: Int = 40, variableFuncGroup: Int = 20,
            forceRunnerEval: Boolean = false, compileEvals: Boolean = false,
            extraConfig: Map[String, String] = Map.empty): Column = {
    com.sparkutils.quality.registerLambdaFunctions( ruleSuite.lambdaFunctions )
    val expressions = flattenExpressions(ruleSuite)
    val collectExpressions =
      if (ddlType.isEmpty)
        expressions.map( i => YamlEncoderExpr(i, renderOptions))
      else
        expressions
    val exprs =
      // ExpressionProxy and SubExprEvaluationRuntime cannot be used with compileEvals
      if (compileEvals)
        collectExpressions.map(PassThroughCompileEvals)
      else
        collectExpressions

    val ddl_type =
      if (ddlType.isEmpty)
        expressionResultTypeYaml
      else
        DataType.fromDDL(ddlType)

    val cleaned = RuleLogicUtils.cleanExprs(ruleSuite)

    ShimUtils.column(ZeroCodeGenWrap.wrap(
      if (forceRunnerEval)
        new ExpressionRunnerEval(cleaned, exprs,
          ddl_type, variablesPerFunc = variablesPerFunc, variableFuncGroup = variableFuncGroup,
          compileEvals = compileEvals, extraConfig = extraConfig)
      else
        new ExpressionRunnerCompiled(cleaned, exprs,
          ddl_type, variablesPerFunc = variablesPerFunc, variableFuncGroup = variableFuncGroup,
          compileEvals = compileEvals, extraConfig = extraConfig)
    )).as(name)
  }
}

private[quality] object ExpressionRunnerUtils {

  protected[quality] def expressionsResultToRow[R](ruleSuiteResult: GeneralExpressionsResult[R]): InternalRow =
    InternalRow(
      packId(ruleSuiteResult.id),
      ArrayBasedMapData(
        ruleSuiteResult.ruleSetResults, packId _, (a: Any) => {
          val v = a.asInstanceOf[Map[VersionedId, GeneralExpressionResult]]
          ArrayBasedMapData(
            v, packId _, (a: Any) => a match {
              case r: GeneralExpressionResult =>
                InternalRow(UTF8String.fromString( r.ruleResult ), UTF8String.fromString( r.resultDDL) )
              case s: String =>  UTF8String.fromString( s )
              case _ => a // handle nulls *and* R's
            }
          )
        }
      )
    )

  def fillDDLs(ar: Array[Any], children: Seq[Expression]): Unit = {
    for( i <- 0 until children.size) {
      ar(i) = UTF8String.fromString(children(i).children.head.dataType.sql)
    }
  }

}

/**
 * Creates an extensible wrapper result column for aggregate expressions, storing the results as yaml
 *
 */
trait ExpressionRunnerBase[T] extends NonSQLExpression with SplitCompilation with TriggerOnly  {

  val ddlType: DataType
  val compileEvals: Boolean

  implicit val classTagT: ClassTag[T]

  lazy val realChildren = getRealChildren(children)

  override def toString: String = s"ExpressionRunner(${ruleSuite.id})" + truncatedString(
    realChildren, "(", ", ", ")", SQLConf.get.maxToStringFields)

  override def nullable: Boolean = false

  // used only for eval, compiled uses the children directly
  lazy val reincorporated = reincorporateExpressions(ruleSuite, realChildren, compileEvals)

  // keep it simple for this one. - can return an internal row or whatever..
  override def eval(input: InternalRow): Any = {
    val res = RuleSuiteFunctions.evalExpressions(reincorporated, input, ddlType)
    ExpressionRunnerUtils.expressionsResultToRow[Any](res)
  }

  /**
   * Used by codegen
   */
  override def createDefaultRuleResult(): InternalRow =
    InternalRow(packTheId(ruleSuite.id),
      ArrayBasedMapData(
        ruleSuite.ruleSets.map{
          ruleSet =>
            packTheId(ruleSet.id) ->
              ArrayBasedMapData(
                ruleSet.rules.map( r => packTheId(r.id) -> null).toMap
              )
        }.toMap
      )
    )

  /**
   * used by codegen
   */
  override def applyNonEmptySuffix: String = "Expression"

  /**
   * used by codegen

  def applyResultExpression(level1: Int, level2: Int, result: InternalRow, ruleResult: Int): Unit =
    applyResult(level1, level2, result, ruleResult.asInstanceOf[Object])
*/
  /**
   * used by codegen
   */
  def applyResultExpression(level1: Int, level2: Int, result: InternalRow, ruleResult: Object): Unit = {
    val sar = result.getMap(1).asInstanceOf[ArrayBasedMapData]
    // update result directly
    val sv = sar.valueArray.asInstanceOf[GenericArrayData]
    sv.getMap(level1).valueArray().asInstanceOf[GenericArrayData].update(level2, ruleResult)
  }

  protected def doGenCodeI(outerCtx: CodegenContext, ev: ExprCode): ExprCode = {

    val SeparateCompilation(clazz, fres, _) =
      SeparateCompilation.withSubExpressions(this,
        Triggers.loadTriggerGrouper(extraConfig).useChildrenForRunner(realChildren), outerCtx, ev, ruleSuite.id,
        topLevelCompilationUnit = true) {
      (ctx, ruleRunnerExpressionIdx, _) =>

        // must be called before the rule gen runs
        val params = genParams(ctx, this)

        val termF = genRuleSuiteTerm[T](ctx, ruleRunnerExpressionIdx)
        // bind the rules
        val utilsName = "com.sparkutils.quality.impl.ExpressionRunnerUtils"

        val ruleRes = "java.lang.Object"
        val strType = classOf[UTF8String].getName
        val ddlArrTerm = ctx.addMutableState(ruleRes + "[]", ctx.freshName("ddlArr"),
          v =>
            if (ddlType == impl.types.expressionResultTypeYaml)
              s"""
                $v = new $strType[${realChildren.size}];\n
                \n
                $utilsName.fillDDLs($v, ${termF._2("realChildren", classOf[Seq[Expression]].getName)});
              """
            else
              s"""
                $v = null;
              """
        )

        def yamlOrType(code: ExprValue, isNull: ExprValue, idx: Int): String =
          if (ddlType == impl.types.expressionResultTypeYaml)
            s"new GenericInternalRow(new Object[]{$code, $ddlArrTerm[$idx]})"
          else
            s"$code"

        val (res, triggerRes) =
          nonOutputRuleGen(ctx, this, ev, utilsName, realChildren, yamlOrType, ruleRunnerExpressionIdx)

      GenerateResult((params, classOf[ExpressionRunnerBase[T]].getName), res, Seq.empty)
    }
    setClazzSource(clazz)
    fres
  }

  override def dataType: DataType =
    expressionsResultsType(ddlType)
}

/**
 * Creates an extensible wrapper result column for aggregate expressions, storing the results as yaml
 *
 */
case class ExpressionRunnerEval(ruleSuite: RuleSuite, children: Seq[Expression], ddlType: DataType,
                            compileEvals: Boolean, variablesPerFunc: Int,
                            variableFuncGroup: Int, extraConfig: Map[String, String], alreadyZero: Boolean = false)
  extends ExpressionRunnerBase[ExpressionRunnerEval] with CodegenFallback {

  override implicit val classTagT: ClassTag[ExpressionRunnerEval] = ClassTag(classOf[ExpressionRunnerEval])

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(children = newChildren)

  override def withZeroCode(): Runner = copy(alreadyZero = true)
}

/**
 * Creates an extensible wrapper result column for aggregate expressions, storing the results as yaml
 *
 */
case class ExpressionRunnerCompiled(ruleSuite: RuleSuite, children: Seq[Expression], ddlType: DataType,
                                compileEvals: Boolean, variablesPerFunc: Int,
                                variableFuncGroup: Int, extraConfig: Map[String, String], alreadyZero: Boolean = false)
  extends ExpressionRunnerBase[ExpressionRunnerCompiled] {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(children = newChildren)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = doGenCodeI(ctx, ev)

  override implicit val classTagT: ClassTag[ExpressionRunnerCompiled] = ClassTag(classOf[ExpressionRunnerCompiled])

  override def withZeroCode(): Runner = copy(alreadyZero = true)
}

case class StripResultTypes(child: Expression) extends UnaryExpression with CodegenFallback with InputTypeChecks {
  protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)

  override def nullSafeEval(input: Any): Any = {
    val row = input.asInstanceOf[InternalRow]
    val setData = row.getMap(1)
    val values =
      Arrays.mapArray(setData.valueArray(), expressionsRuleSetType(StringType), a => {
        val rulesData = a.asInstanceOf[MapData]
        val values = Arrays.mapArray(rulesData.valueArray(), expressionResultTypeYaml, a => {
          val row = a.asInstanceOf[InternalRow]
          row.getUTF8String(0)
        })
        new ArrayBasedMapData(rulesData.keyArray(), new GenericArrayData(values))
      })
    InternalRow(row.getLong(0), new ArrayBasedMapData(setData.keyArray(), new GenericArrayData(values)))
  }

  override def dataType: DataType = expressionsResultsNoDDLType

  override def inputDataTypes: Seq[Seq[DataType]] = Seq(Seq(expressionsResultsType(expressionResultTypeYaml)))
}