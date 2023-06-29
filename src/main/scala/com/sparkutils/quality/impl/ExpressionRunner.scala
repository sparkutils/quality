package com.sparkutils.quality.impl

import com.sparkutils.quality.impl.ExpressionRunner.expressionsResultToRow
import com.sparkutils.quality.impl.RuleRunnerUtils.{flattenExpressions, reincorporateExpressions}
import com.sparkutils.quality._
import org.apache.spark.sql.QualitySparkUtils.cast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression}
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.sql.Column
import org.apache.spark.unsafe.types.UTF8String

object ExpressionRunner {
  /**
   * Runs the ruleSuite expressions saving results as a tuple of (ruleResult: String, resultType: String)
   *
   * @param ruleSuite
   * @param name
   * @return
   */
  def apply(ruleSuite: RuleSuite, name: String = "expressionResults"): Column = {
    com.sparkutils.quality.registerLambdaFunctions( ruleSuite.lambdaFunctions )
    val expressions = flattenExpressions(ruleSuite)
    val collectExpressions = expressions.map( i => cast(i, StringType))
    new Column(ExpressionRunner(ruleSuite, collectExpressions)).as(name)
  }

  protected[quality] def expressionsResultToRow(ruleSuiteResult: GeneralExpressionsResult): InternalRow =
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
    val res = reincorporated.evalExpressions(input)
    expressionsResultToRow(res)
  }

  // not really worth it in this case for a single row.
  // override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = ???

  override def dataType: DataType = expressionsResultsType

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)

}