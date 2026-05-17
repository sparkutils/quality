package com.sparkutils.quality.impl

import com.sparkutils.quality.impl.RuleRunnerUtils.packTheId
import com.sparkutils.quality.impl.RuleSuiteHelpers.getContextOrSparkClassLoader
import com.sparkutils.quality.impl.util.{TopLevelBoolean, TopLevelBooleanSuiteBuilder}
import com.sparkutils.quality.{FailedInt, QualityException, RuleSuite, UnevaluatedRule, UnevaluatedRuleInt, getConfig, groupProcessorAuditKey, groupProcessorBucketSizeKey, groupProcessorKey, groupProcessorPercentFilter}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{Block, CodegenContext, QualityCodeGenUtils, QualityExprUtils}
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
            variablesPerFunc: Int, variableFuncGroup: Int, paramsDef: String, paramsCall: String):  (Iterator[String], String) =
    apply(
      ctx: CodegenContext, expressions: Seq[(Trigger, Block)],
      variablesPerFunc: Int, variableFuncGroup: Int, paramsDef: String, paramsCall: String,
      prefix = "ruleRunner", exprEnd = () => code"",
      exprFunEnd = () => code"", Map.empty
    )

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
  def dumpAudit(runner: HasTriggers): Unit

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

  def dumpAudit(runner: HasTriggers): Unit = {}
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
      // hopefully doesn't generate again
      val subExprs = ctx.subexpressionEliminationForWholeStageCodegen(groupExprs ++
        QualityExprUtils.currentSubExprState(ctx).map(_._1.e))
      val subExpressionCode = QualityExprUtils.evaluateSubExprEliminationState(ctx, subExprs)

      (QualityCodeGenUtils.withSubExprEliminationExprs(ctx, subExprs.states) {
        builder
      }, subExpressionCode)
    }
  }

  override def dumpAudit(runner: HasTriggers): Unit = {
    TopLevelBooleanSuiteBuilder.build(runner)
  }

}

trait Runner extends Expression {

  val ruleSuite: RuleSuite
  val extraConfig: Map[String, String]
  val variablesPerFunc: Int
  val variableFuncGroup: Int

  val defaultRuleResult: Int
  val defaultOverallResult: Int

  /**
   * Used by compilation
   * @return
   */
  def createDefaultRuleResult(): InternalRow =
    InternalRow(packTheId(ruleSuite.id), defaultOverallResult,
      ArrayBasedMapData(
        ruleSuite.ruleSets.map{
          ruleSet =>
            packTheId(ruleSet.id) -> InternalRow(defaultOverallResult,
              ArrayBasedMapData(
                ruleSet.rules.map( r => packTheId(r.id) -> defaultRuleResult).toMap
              ))
        }.toMap
      )
    )

  val defaultOverallProcessor: (Int, Int, Double) => Int

  // only used for compilation
  def inPlaceArrayOffsets: Array[RuleRunnerUtils.InPlaceOffset] = RuleRunnerUtils.inPlaceArrayOffsets(ruleSuite, defaultOverallProcessor)

}

/**
 * Base class for runners that use triggers_ collector, engine and folder
 */
trait HasTriggers extends Runner {

  val defaultRuleResult: Int = UnevaluatedRuleInt
  val defaultOverallResult: Int = FailedInt
  val defaultOverallProcessor: (Int, Int, Double) => Int = OverallResultHelper.inplaceForDefaultInt

  val triggerCount: Int

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

  val audited: Boolean

  def performGroupingAuditDump(): Unit = {
    if (shouldAudit && canAudit) {

      Triggers.loadTriggerGrouper(extraConfig).dumpAudit(this)

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