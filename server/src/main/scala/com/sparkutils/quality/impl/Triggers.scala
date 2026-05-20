package com.sparkutils.quality.impl

import com.sparkutils.quality.impl.RuleRunnerUtils.packTheId
import com.sparkutils.quality.impl.RuleSuiteHelpers.getContextOrSparkClassLoader
import com.sparkutils.quality.impl.util.{TopLevelBoolean, TopLevelBooleanSuiteBuilder}
import com.sparkutils.quality.{FailedInt, QualityException, RuleSuite, UnevaluatedRule, UnevaluatedRuleInt, getConfig, groupProcessorAuditKey, groupProcessorBucketSizeKey, groupProcessorKey, groupProcessorPercentFilter}
import com.sparkutils.shim.codegen.SubExprCodeGen
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{Block, CodegenContext, QualityCodeGenUtils, ShimExprUtils}
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData

import scala.runtime.AbstractFunction10
import scala.util.Try

case class Trigger(expression: Expression, index: Int, salience: Int)

case class Group(groupFilter: Expression, lowestSalience: Int, triggers: Seq[Trigger])

/**
 * Allow customised grouping of runner triggers, DQ and ExpressionRunner should evaluate all so the default
 * implementation is sufficient.  This abstraction was added as part of #129 due to 20k trigger rules.
 *
 * The return type is the list of function names to call and any extra common subexpressions needed to group
 */
trait TriggerGrouper extends AbstractFunction10[CodegenContext, Seq[(Trigger, Block)],
  Int, Int, String, String, String, () => Block, () => Block, Map[String, String],  (Iterator[String], String)] {

  def apply(ctx: CodegenContext, expressions: Seq[(Trigger, Block)],
            variablesPerFunc: Int, variableFuncGroup: Int, paramsDef: String, paramsCall: String,
            prefix: String, exprEnd: () => Block, exprFunEnd: () => Block,
            extraConfig: Map[String, String]): (Iterator[String], String)

  /**
   * provides a dump of the plan with defaults or provided by extraConfig and by any optimisation results.
   * This is designed to run in withNewChildren, typically on the first call, when resolved == true, allowing
   * for an audit friendly version of the rules to be examined.  As this takes place during the Spark analysis phase
   * it is recommended that the binary RuleSuiteGroup format is used.
   *
   * The binary results should not be used across Quality releases and may contain "boundreference()" entries instead of
   * actual field names.
   *
   * The DefaultTriggerGrouper does not have any output.
   */
  def dumpAudit(runner: HasOutput): Unit

}

case class DefaultTriggerGrouper() extends TriggerGrouper {

  override def apply(
                      ctx: CodegenContext, expressions: Seq[(Trigger, Block)],
                      variablesPerFunc: Int, variableFuncGroup: Int, paramsDef: String, paramsCall: String,
                      prefix: String, exprEnd: () => Block, exprFunEnd: () => Block, extraConfig: Map[String,String]):
    (Iterator[String], String)= {

    val allExpr = expressions.map(_._2).grouped(variablesPerFunc).grouped(variableFuncGroup)

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
    (funNames, "")
  }

  def dumpAudit(runner: HasOutput): Unit = {}
}

/**
 * Groups by common top level Boolean And and EqualTo expressions with Literals, using buckets of hashes on the literal
 * values.  Using this approach can lead to a 10x spread increase over the default grouper for very large truth table
 * style rules (tested against the 20k_rule_suite.csv in the BigRules testsuite).
 *
 * Only supported with Spark 3.2 and above
 */
case class TopLevelBooleanGrouper() extends TriggerGrouper {

  override def apply(
                      ctx: CodegenContext, expressions: Seq[(Trigger, Block)],
                      variablesPerFunc: Int, variableFuncGroup: Int, paramsDef: String, paramsCall: String,
                      prefix: String, exprEnd: () => Block, exprFunEnd: () => Block, extraConfig: Map[String,String]):
    (Iterator[String], String) = {

    val targetBucket = Try(Triggers.getValue(groupProcessorBucketSizeKey, extraConfig, "130").toInt).
      getOrElse(130)
    val triggerPercentFilter = Try(Triggers.getValue(groupProcessorPercentFilter, extraConfig, "0.12").toDouble).
      getOrElse(0.12)

    val map = expressions.toMap

    val groups = TopLevelBoolean.bucket(expressions.map(_._1), targetBucket, triggerPercentFilter)
    val simpleGrouper = DefaultTriggerGrouper()

    val groupExprs = groups.map(_.groupFilter)

    def builder = {
      val grouped = groups.grouped(variablesPerFunc).grouped(variableFuncGroup)

      val funNames =
        for (exprGroup <- grouped) yield {
          val groupName = ctx.freshName(prefix + "GEGroup")
          ctx.addNewFunction(groupName, {
            val funNames =
              for {
                exprFunc <- exprGroup
              } yield {
                val exprFuncName = ctx.freshName(prefix + "GEFuncGroup")

                val groupCalls =
                  exprFunc.map {
                    group =>
                      // group the group, there is no sub expression usage for common
                      val funNames =
                        simpleGrouper(ctx, group.triggers.map(t => (t, map(t))), variablesPerFunc,
                          variableFuncGroup, paramsDef, paramsCall, prefix, exprEnd, exprFunEnd, extraConfig)._1

                      val eval = group.groupFilter.genCode(ctx)
                      code"""
                      ${exprEnd()}\n
                      ${eval.code}
                      if ((!${eval.isNull}) && ${eval.value}  ) {
                        ${funNames.map { f => s"$f($paramsCall);" }.mkString("\n")}
                      }
                    """
                  }

                ctx.addNewFunction(exprFuncName,
                  code"""
   private void $exprFuncName($paramsDef) {
     ${groupCalls.mkString(s"\n")}
   }
  """.code
                )
              }

            code"""
   private void $groupName($paramsDef) {
     ${exprFunEnd()}
     ${funNames.map { f => s"$f($paramsCall);" }.mkString(s"${exprFunEnd()}\n")}
   }
   """.code

          })
        }
      val s = funNames.toSeq
      s.iterator
    }

    if (ctx.currentVars eq null) {
      // only fails on "via ProcessFactory with Avro inputs" RowToRowTest shows it doesn't always work for projections

      val subExpressionCode = QualityCodeGenUtils.nonWholeStageSubexpressionElimination(ctx, groupExprs)

      (builder, subExpressionCode)
    } else {
      // will generate again, the sub exprs will be present on the projection unless ZeroCodeGen is enabled
      val subExprs = SubExprCodeGen.subexpressionEliminationForWholeStageCodegen(ctx, groupExprs ++
        ShimExprUtils.currentSubExprState(ctx).map(s => ShimExprUtils.fromState(s._1)))
      val subExpressionCode = ShimExprUtils.evaluateSubExprEliminationState(ctx, subExprs)

      (QualityCodeGenUtils.withSubExprEliminationExprs(ctx, subExprs.states) {
        builder
      }, subExpressionCode)
    }
  }

  override def dumpAudit(runner: HasOutput): Unit = {
    TopLevelBooleanSuiteBuilder.build(runner)
  }

}

object Triggers {

  def getValue(key: String, extraConfig: Map[String, String], default: String): String =
    extraConfig.get(key).orElse(
      Option(getConfig(key, default = null))
    ).getOrElse(default)

  def loadTriggerGrouper(extraConfig: Map[String, String]): TriggerGrouper = {
    val name = getValue(groupProcessorKey, extraConfig, classOf[DefaultTriggerGrouper].getName)

    val impl =
      try {
        Class.forName(name, false, getContextOrSparkClassLoader).newInstance().asInstanceOf[TriggerGrouper]
      } catch {
        case t: Throwable => throw QualityException(s"Could not load TriggerGrouper of name $name", t)
      }
    impl
  }

}