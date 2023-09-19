package com.sparkutils.quality.impl

import com.sparkutils.quality.impl.ExpressionRunner.expressionsResultToRow
import com.sparkutils.quality.impl.RuleRunnerUtils.{flattenExpressions, reincorporateExpressions}
import com.sparkutils.quality._
import com.sparkutils.quality.impl.imports.RuleResultsImports
import com.sparkutils.quality.impl.imports.RuleResultsImports.packId
import com.sparkutils.quality.impl.util.Arrays
import com.sparkutils.quality.impl.yaml.YamlEncoderExpr
import types._
import org.apache.spark.sql.QualitySparkUtils.cast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression, UnaryExpression}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData, MapData}
import org.apache.spark.sql.types.{DataType, DataTypes, StringType}
import org.apache.spark.sql.Column
import org.apache.spark.sql.qualityFunctions.InputTypeChecks
import org.apache.spark.unsafe.types.UTF8String

object ExpressionRunner {
  /**
   * Runs the ruleSuite expressions saving results as a tuple of (ruleResult: yaml, resultType: String)
   *
   * @param ruleSuite
   * @param name
   * @return
   */
  def apply(ruleSuite: RuleSuite, name: String = "expressionResults", renderOptions: Map[String, String] = Map.empty, ddlType: String = ""): Column = {
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

    new Column(ExpressionRunner(RuleLogicUtils.cleanExprs(ruleSuite), collectExpressions, ddl_type)).as(name)
  }

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
}

/**
 * Creates an extensible wrapper result column for aggregate expressions, storing the results as yaml
 *
 * @param ruleSuite
 * @param children
 */
case class ExpressionRunner(ruleSuite: RuleSuite, children: Seq[Expression], ddlType: DataType)
  extends Expression with CodegenFallback with NonSQLExpression {
  override def nullable: Boolean = false

  // used only for eval, compiled uses the children directly
  lazy val reincorporated = reincorporateExpressions(ruleSuite, children, false)

  // keep it simple for this one. - can return an internal row or whatever..
  override def eval(input: InternalRow): Any = {
    val res = RuleSuiteFunctions.evalExpressions(reincorporated, input, ddlType)
    expressionsResultToRow[Any](res)
  }

  // not really worth it in this case for a single row.
  // override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = ???

  override def dataType: DataType = expressionsResultsType(ddlType)

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