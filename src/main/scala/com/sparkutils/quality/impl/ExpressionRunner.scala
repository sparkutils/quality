package com.sparkutils.quality.impl

import com.sparkutils.quality
import com.sparkutils.quality._
import com.sparkutils.quality.impl.RuleRunnerUtils.{PassedInt, RuleSuiteResultArray, flattenExpressions, genRuleSuiteTerm, nonOutputRuleGen, reincorporateExpressions}
import com.sparkutils.quality.impl.imports.RuleResultsImports.packId
import com.sparkutils.quality.impl.util.Arrays
import com.sparkutils.quality.impl.yaml.YamlEncoderExpr
import com.sparkutils.quality.types._
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode, ExprValue}
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression, UnaryExpression}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData, MapData}
import org.apache.spark.sql.shim.expressions.InputTypeChecks
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

object ExpressionRunner {
  /**
   * Runs the ruleSuite expressions saving results as a tuple of (ruleResult: yaml, resultType: String)
   *
   * @param ruleSuite
   * @param name
   * @return
   */
  def apply(ruleSuite: RuleSuite, name: String = "expressionResults", renderOptions: Map[String, String] = Map.empty, ddlType: String = "",
            variablesPerFunc: Int = 40, variableFuncGroup: Int = 20, forceRunnerEval: Boolean = false, compileEvals: Boolean = true): Column = {
    com.sparkutils.quality.registerLambdaFunctions( ruleSuite.lambdaFunctions )
    val expressions = flattenExpressions(ruleSuite)
    val collectExpressions =
      if (ddlType.isEmpty)
        expressions.map( i => YamlEncoderExpr(i, renderOptions))
      else
        expressions

    val ddl_type =
      if (ddlType.isEmpty)
        expressionResultTypeYaml
      else
        DataType.fromDDL(ddlType)

    new Column(ExpressionRunner(RuleLogicUtils.cleanExprs(ruleSuite), collectExpressions,
      ddl_type, variablesPerFunc = variablesPerFunc, variableFuncGroup = variableFuncGroup,
      compileEvals = compileEvals, forceRunnerEval = forceRunnerEval)).as(name)
  }
}

private[quality] object ExpressionRunnerUtils {

  protected[quality] def expressionsResultToRow[R](ruleSuiteResult: GeneralExpressionsResult[R]): InternalRow =
    InternalRow(
      packId(ruleSuiteResult.id),
      ArrayBasedMapData(
        ruleSuiteResult.ruleSetResults, packId, (a: Any) => {
          val v = a.asInstanceOf[Map[VersionedId, GeneralExpressionResult]]
          ArrayBasedMapData(
            v, packId, (a: Any) => a match {
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
      ar(i) = UTF8String.fromString(children(i).children(0).dataType.sql)
    }
  }

  // ruleSuite is not used, its for compat with nonOutputRuleGen
  def evalArray(ruleSuite: RuleSuite, ruleSuiteArrays: RuleSuiteResultArray, results: Array[Any]): InternalRow = {
    val ruleSetRes = Array.ofDim[ArrayBasedMapData](ruleSuiteArrays.ruleSetIds.length)

    var offset = 0

    for( rsi <- ruleSuiteArrays.ruleSetIds.indices) {

      val rulesetSize = ruleSuiteArrays.ruleSets(rsi).length

      val ruleSetResults = results.slice(offset, offset + rulesetSize)
      offset += rulesetSize

      ruleSetRes(rsi) =
        ArrayBasedMapData( ruleSuiteArrays.ruleSets(rsi), ruleSetResults )

    }

    InternalRow( ruleSuiteArrays.packedId,
      ArrayBasedMapData( ruleSuiteArrays.ruleSetIds, ruleSetRes)
    )
  }

}

/**
 * Creates an extensible wrapper result column for aggregate expressions, storing the results as yaml
 *
 * @param ruleSuite
 * @param children
 */
case class ExpressionRunner(ruleSuite: RuleSuite, children: Seq[Expression], ddlType: DataType,
                            compileEvals: Boolean, variablesPerFunc: Int,
                            variableFuncGroup: Int, forceRunnerEval: Boolean)
  extends Expression with CodegenFallback with NonSQLExpression {
  override def nullable: Boolean = false

  // used only for eval, compiled uses the children directly
  lazy val reincorporated = reincorporateExpressions(ruleSuite, children, compileEvals)

  // keep it simple for this one. - can return an internal row or whatever..
  override def eval(input: InternalRow): Any = {
    val res = RuleSuiteFunctions.evalExpressions(reincorporated, input, ddlType)
    ExpressionRunnerUtils.expressionsResultToRow[Any](res)
  }

  // TODO - fill this in...
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    if (forceRunnerEval) {
      return super[CodegenFallback].doGenCode(ctx, ev)
    }
    val i = ctx.INPUT_ROW

    ctx.references += this

    val termF = genRuleSuiteTerm[ExpressionRunner](ctx)
    // bind the rules
    val ruleSuitTerm = termF._1
    val utilsName = "com.sparkutils.quality.impl.ExpressionRunnerUtils"

    val ruleRes = "java.lang.Object"
    val strType = classOf[UTF8String].getName
    val ddlArrTerm = ctx.addMutableState(ruleRes+"[]", ctx.freshName("ddlArr"),
      v =>
        if (ddlType == quality.types.expressionResultTypeYaml)
          s"""
            $v = new $strType[${children.size}];\n
            \n
            $utilsName.fillDDLs($v, ${termF._2("children", classOf[Seq[Expression]].getName)});
          """
        else
          s"""
            $v = null;
          """
    )

    def yamlOrType(code: ExprValue, idx: Int): String =
      if (ddlType == quality.types.expressionResultTypeYaml)
        s"new GenericInternalRow(new Object[]{$code, $ddlArrTerm[$idx]})"
      else
        s"$code"

    nonOutputRuleGen(ctx, ev, i, ruleSuitTerm, utilsName, children, variablesPerFunc, variableFuncGroup,
      yamlOrType(_,_)
    )
  }

  override def dataType: DataType =
    expressionsResultsType(ddlType)

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)

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