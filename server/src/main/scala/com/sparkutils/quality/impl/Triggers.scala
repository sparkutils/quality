package com.sparkutils.quality.impl

import com.sparkutils.quality.impl.RuleEngineRunnerUtils.flattenSalience
import com.sparkutils.quality.impl.RuleSuiteHelpers.getContextOrSparkClassLoader
import com.sparkutils.quality.impl.util.{TopLevelBoolean, TopLevelBooleanSuiteBuilder}
import com.sparkutils.quality.{QualityException, RuleSuite, getConfig, groupProcessorAuditKey, groupProcessorBucketSizeKey, groupProcessorKey, groupProcessorPercentFilter}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{Block, CodegenContext}

import scala.runtime.AbstractFunction10
import scala.util.Try

case class Trigger(expression: Expression, index: Int, salience: Int)

case class Group(groupFilter: Expression, lowestSalience: Int, triggers: Seq[Trigger])

/**
 * Allow customised grouping of runner triggers, DQ and ExpressionRunner should evaluate all so the default
 * implementation is sufficient.  This abstraction was added as part of #129 due to 20k trigger rules.
 */
trait TriggerGrouper extends AbstractFunction10[CodegenContext, Seq[(Trigger, Block)],
  Int, Int, String, String, String, () => Block, () => Block, Map[String, String], Iterator[String]] {

  def apply(ctx: CodegenContext, expressions: Seq[(Trigger, Block)],
            variablesPerFunc: Int, variableFuncGroup: Int, paramsDef: String, paramsCall: String): Iterator[String] =
    apply(
      ctx: CodegenContext, expressions: Seq[(Trigger, Block)],
      variablesPerFunc: Int, variableFuncGroup: Int, paramsDef: String, paramsCall: String,
      prefix = "ruleRunner", exprEnd = () => code"",
      exprFunEnd = () => code"", Map.empty
    )

  def apply(ctx: CodegenContext, expressions: Seq[(Trigger, Block)],
            variablesPerFunc: Int, variableFuncGroup: Int, paramsDef: String, paramsCall: String,
            prefix: String, exprEnd: () => Block, exprFunEnd: () => Block,
            extraConfig: Map[String, String]): Iterator[String]

  /**
   * provides a dump of the plan with defaults or provided by extraConfig and by any optimisation results.
   * This is designed to run in withNewChildren, typically on the first call, when resolved == true, allowing
   * for an audit friendly version of the rules to be examined.  As this takes place during the Spark analysis phase
   * it is recommended that the binary RuleSuiteGroup format is used.
   *
   * The DefaultTriggerGrouper does not have any output.
   */
  def dumpAudit(runner: HasTriggers): Unit

}


case class DefaultTriggerGrouper() extends TriggerGrouper {

  override def apply(
                      ctx: CodegenContext, expressions: Seq[(Trigger, Block)],
                      variablesPerFunc: Int, variableFuncGroup: Int, paramsDef: String, paramsCall: String,
                      prefix: String, exprEnd: () => Block, exprFunEnd: () => Block, extraConfig: Map[String,String]):
  Iterator[String] = {

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
    funNames
  }

  def dumpAudit(runner: HasTriggers): Unit = {}
}

case class TopLevelBooleanGrouper() extends TriggerGrouper {

  override def apply(
                      ctx: CodegenContext, expressions: Seq[(Trigger, Block)],
                      variablesPerFunc: Int, variableFuncGroup: Int, paramsDef: String, paramsCall: String,
                      prefix: String, exprEnd: () => Block, exprFunEnd: () => Block, extraConfig: Map[String,String]):
  Iterator[String] = {

    val targetBucket = Try(Triggers.getValue(groupProcessorBucketSizeKey, extraConfig, "130").toInt).
      getOrElse(130)
    val triggerPercentFilter = Try(Triggers.getValue(groupProcessorPercentFilter, extraConfig, "0.12").toDouble).
      getOrElse(0.12)

    val map = expressions.toMap

    val groups = TopLevelBoolean.bucket(expressions.map(_._1), targetBucket, triggerPercentFilter)
    val simpleGrouper = DefaultTriggerGrouper()

    val grouped = groups.grouped(variablesPerFunc).grouped(variableFuncGroup)

    val funNames =
      for (exprGroup <- grouped) yield {
        val groupName = ctx.freshName(prefix+"GEGroup")
        ctx.addNewFunction(groupName, {
          val funNames =
            for {
              exprFunc <- exprGroup
            } yield {
              val exprFuncName = ctx.freshName(prefix+"GEFuncGroup")

              val groupCalls =
                exprFunc.map {
                  group =>
                    // group the group
                    val funNames =
                      simpleGrouper(ctx, group.triggers.map(t => (t, map(t))), variablesPerFunc,
                        variableFuncGroup, paramsDef, paramsCall, prefix, exprEnd, exprFunEnd, extraConfig)

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
     ${exprFunEnd()}
     ${groupCalls.mkString(s"${exprFunEnd()}\n")}
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
    funNames
  }

  override def dumpAudit(runner: HasTriggers): Unit = {
    TopLevelBooleanSuiteBuilder.build(runner)
  }

}

/**
 * Base class for runners that use triggers_ collector, engine and folder
 */
trait HasTriggers {

  val ruleSuite: RuleSuite
  val triggerCount: Int
  val extraConfig: Map[String, String]
  val variablesPerFunc: Int
  val variableFuncGroup: Int

  def realChildren: Seq[Expression]

  def triggerRules: Seq[Expression] = realChildren.slice(0, triggerCount)

  def canAudit: Boolean = triggerRules.forall(_.resolved)

  /**
   * For collector and engine it's typically just their sql function name, for folder
   * it must also include the starting expression.sql.  This will be called with resolved
   * expressions but not with structs so the folder starter expression is expected to be
   * executable
   * @param ruleSuiteCall provided by the grouping code but resolves to a rulesuite
   * @return
   */
  def groupedSqlCall(ruleSuiteCall: String): String

  private lazy val shouldAudit = Try(Triggers.getValue(groupProcessorAuditKey, extraConfig, "false").toBoolean).getOrElse(false)

  private var audited: Boolean = false

  def performGroupingAuditDump(): Unit = {
    if (shouldAudit && !audited && canAudit) {

      Triggers.loadTriggerGrouper(extraConfig).dumpAudit(this)

      audited = true
    }
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