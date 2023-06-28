package com.sparkutils.quality.impl

import com.sparkutils.quality.impl.ExpressionRunner.expressionsResultToRow
import com.sparkutils.quality.impl.RuleRegistrationFunctions.flattenExpressions
import com.sparkutils.quality.impl.RuleRunnerUtils.reincorporateExpressions
import com.sparkutils.quality.{GeneralExpressionResult, GeneralExpressionsResult, RuleSuite, VersionedId, expressionsResultsType, packId}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.QualitySparkUtils.cast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, NonSQLExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

object ExpressionRunner {
  def run(ruleSuite: RuleSuite, dataFrame: DataFrame, name: String = "expressionResults"): DataFrame = {
    com.sparkutils.quality.registerLambdaFunctions( ruleSuite.lambdaFunctions )
    val expressions = flattenExpressions(ruleSuite)
    val res = dataFrame.select(expressions.zipWithIndex.map( p => new Column(p._1).as(s"f_${p._2}")) :_*)
    // cast all the results to string
    val collectExpressions = expressions.indices.map( i => cast(col(s"f_$i").expr, StringType))
    res.select(new Column(ExpressionRunner(ruleSuite, collectExpressions)).as(name))
  }

  def expressionsResultToRow(ruleSuiteResult: GeneralExpressionsResult): InternalRow =
    InternalRow(
      packId(ruleSuiteResult.id),
      ArrayBasedMapData(
        ruleSuiteResult.ruleSetResults, packId, (a: Any) => {
          val v = a.asInstanceOf[Map[VersionedId, GeneralExpressionResult]]
          ArrayBasedMapData(
            v, packId, (a: Any) => {
              val r = a.asInstanceOf[GeneralExpressionResult]
              InternalRow(UTF8String.fromString( r.ruleResult ), UTF8String.fromString( r.resultType) )
            }
          )
        }
      )
    )
}

/**
 * Creates an extensible wrapper result column for aggregate expressions, adding casts as needed to string
 *
 * @param ruleSuite
 * @param children
 */
case class ExpressionRunner(ruleSuite: RuleSuite, children: Seq[Expression])
  extends Expression with CodegenFallback with NonSQLExpression {
  override def nullable: Boolean = false

  // used only for eval, compiled uses the children directly
  lazy val reincorporated = reincorporateExpressions(ruleSuite, children, false)

  // keep it simple for this one. - can return an internal row or whatever..
  override def eval(input: InternalRow): Any = {
    val res = reincorporated.evalAggregates(input)
    expressionsResultToRow(res)
  }

  // not really worth it in this case.
  // override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = ???

  override def dataType: DataType = expressionsResultsType

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)

}