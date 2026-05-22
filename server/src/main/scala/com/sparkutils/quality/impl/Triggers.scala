package com.sparkutils.quality.impl

import com.sparkutils.quality.impl.RuleRunnerUtils.packTheId
import com.sparkutils.quality.impl.RuleSuiteHelpers.getContextOrSparkClassLoader
import com.sparkutils.quality.impl.util.Params.formatParams
import com.sparkutils.quality.impl.util.{ParameterInformation, SeparateClassGenerator, SeparateCompilation, SubCompilation, TopLevelBoolean, TopLevelBooleanSuiteBuilder}
import com.sparkutils.quality.{FailedInt, QualityException, RuleSuite, UnevaluatedRule, UnevaluatedRuleInt, getConfig, groupProcessorAuditKey, groupProcessorBucketSizeKey, groupProcessorKey, groupProcessorPercentFilter}
import com.sparkutils.shim.codegen.SubExprCodeGen
import org.apache.spark.sql.ClassicQualitySparkUtils.{genParams, genParamsForNested}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow}
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{Block, CodeAndComment, CodeGenerator, CodegenContext, ExprCode, ExprValue, FalseLiteral, GlobalValue, QualityCodeGenUtils, ShimExprUtils, VariableValue}
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData

import scala.runtime.{AbstractFunction10, AbstractFunction11, AbstractFunction12, AbstractFunction13}
import scala.util.Try

case class Trigger(expression: Expression, index: Int, salience: Int, outputExpression: Option[Expression] = None)

case class Group(groupFilter: Expression, lowestSalience: Int, triggers: Seq[Trigger])

/**
 * Allow customised grouping of runner triggers, DQ and ExpressionRunner should evaluate all so the default
 * implementation is sufficient.  This abstraction was added as part of #129 due to 20k trigger rules.
 *
 * The return type is the list of function names to call and any extra common subexpressions needed to group
 */
trait TriggerGrouper extends AbstractFunction12[CodegenContext, Runner, String, Seq[VariableValue],
  Seq[(Trigger, (CodegenContext, ParameterInformation) => Block)],
  Int, Int, ParameterInformation, String, () => Block, () => Block, Map[String, String], (Iterator[String], String, Seq[CodeAndComment])] {

  def apply(ctx: CodegenContext, runner: Runner, resultRow: String, additionalParams: Seq[VariableValue],
            expressions: Seq[(Trigger, (CodegenContext, ParameterInformation) => Block)],
            variablesPerFunc: Int, variableFuncGroup: Int, params: ParameterInformation,
            prefix: String, exprEnd: () => Block, exprFunEnd: () => Block,
            extraConfig: Map[String, String]): (Iterator[String], String, Seq[CodeAndComment])

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

  override def apply( ctx: CodegenContext, runner: Runner, resultRow: String, additionalParams: Seq[VariableValue],
                      expressions: Seq[(Trigger, (CodegenContext, ParameterInformation) => Block)],
                      variablesPerFunc: Int, variableFuncGroup: Int, params: ParameterInformation,
                      prefix: String, exprEnd: () => Block, exprFunEnd: () => Block, extraConfig: Map[String,String]):
    (Iterator[String], String, Seq[CodeAndComment])= {

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
   private void $exprFuncName(${params.paramsDef}) {
     ${exprFunc.map(_.apply(ctx, params)).mkString(s"${exprEnd()}\n")}
   }
  """.code
              )
            }

          code"""
   private void $groupName(${params.paramsDef}) {
     ${funNames.map { f => s"$f(${params.paramsCall});" }.mkString(s"${exprFunEnd()}\n")}
   }
   """.code

        })
      }
    (funNames, "", Seq.empty[CodeAndComment])
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

  override def apply( ctx: CodegenContext, runner: Runner, resultRow: String, additionalParams: Seq[VariableValue],
                      expressions: Seq[(Trigger, (CodegenContext, ParameterInformation) => Block)],
                      variablesPerFunc: Int, variableFuncGroup: Int, params: ParameterInformation,
                      prefix: String, exprEnd: () => Block, exprFunEnd: () => Block, extraConfig: Map[String,String]):
    (Iterator[String], String, Seq[CodeAndComment]) = {

    val targetBucket = Try(Triggers.getValue(groupProcessorBucketSizeKey, extraConfig, "130").toInt).
      getOrElse(130)
    val triggerPercentFilter = Try(Triggers.getValue(groupProcessorPercentFilter, extraConfig, "0.12").toDouble).
      getOrElse(0.12)

    val map = expressions.toMap

    val groups = TopLevelBoolean.bucket(expressions.map(_._1), targetBucket, triggerPercentFilter)
    val simpleGrouper = DefaultTriggerGrouper()

    val groupExprs = groups.map(_.groupFilter)

    def builder = {

      val resExpr = Seq(VariableValue(resultRow, classOf[InternalRow]))

      val grouped = groups.zipWithIndex.grouped(variablesPerFunc).grouped(variableFuncGroup)

      val funPairs =
        for (exprGroup <- grouped) yield {
          val groupName = ctx.freshName(prefix + "GEGroup")

          val subClasses =
            for {
              exprFunc <- exprGroup
            } yield {
              val exprFuncName = ctx.freshName(prefix + "GEFuncGroup")

              val groupCalls =
                exprFunc.map {
                  case (group, groupIndex) =>

                    val id = s"Group$groupIndex"
                    //val clazzName = SeparateCompilation.className(id)

                    val allGroupExprs = group.triggers.flatMap{
                      trigger =>
                        Seq(trigger.expression) ++ trigger.outputExpression.map(Seq(_)).getOrElse(Seq.empty)
                    }

                    //TODO - subexprs that are small enough to inline won't show as local
                    // remove the params usage, everything is in the object variables
                    val preCalcParams = (ctx: CodegenContext) =>
                      genParamsForNested(ctx, allGroupExprs, additionalParams).copy(paramsDef = "", paramsCall = "")

                    val resCode = ExprCode(VariableValue(ctx.freshName("groupResultNull"), java.lang.Boolean.TYPE),
                      VariableValue(ctx.freshName("groupResult"), classOf[GenericInternalRow]))
                    val (body, expr) =
                      SeparateCompilation.withSubExpressions(runner, allGroupExprs, ctx, resCode,
                        SubCompilation(id, s"Trigger group $groupIndex"),
                        // we need to pipe the row in
                        extraParams = additionalParams,
                        useParams = preCalcParams
                      ) { (ctx, index, params) =>
                        // group the group, there is no sub expression usage for common

                        val grpResult = ctx.freshName("groupResult")

                        val sgr = simpleGrouper(ctx, runner, grpResult, additionalParams,
                          group.triggers.map(t => (t, map(t))), variablesPerFunc,
                          variableFuncGroup, params, prefix, exprEnd, exprFunEnd, extraConfig)

                        val funNames = sgr._1
                        val exprRunner =
                          ExprCode(VariableValue(ctx.freshName("groupResultNull"), java.lang.Boolean.TYPE),
                            VariableValue(grpResult, classOf[GenericInternalRow])
                          )

                        // the top level is 0 arrays are filtered out from the row as they don't need explicit returning
                        (SeparateClassGenerator(runner.getClass.getName, additionalParams, groupIndex + 1), exprRunner.copy(
                          code = code"""
                              boolean ${exprRunner.isNull} = false;
                              ${funNames.map { f => s"$f(${params.paramsCall});" }.mkString("\n")}
                              GenericInternalRow ${exprRunner.value} = new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(
                                new Object[]{
                                ${additionalParams.filterNot(_.javaType.isArray).map(_.variableName).mkString(",\n")}
                                }
                              );
                              """
                        ), sgr._3)
                      }

                    val eval = group.groupFilter.genCode(ctx)
                    (code"""
                        ${exprEnd()}\n
                        ${eval.code}
                        if ((!${eval.isNull}) && ${eval.value}  ) {
                          ${expr.code}
                        }
                      """, body)
                }

              (ctx.addNewFunction(exprFuncName,
                code"""
                 private void $exprFuncName(${params.paramsDef}) {
                   ${groupCalls.map(_._1).mkString(s"\n")}
                 }
                """.code
              ), groupCalls.map(_._2))
            }

          (ctx.addNewFunction(groupName,

            code"""
             private void $groupName(${params.paramsDef}) {
               ${exprFunEnd()}
               ${subClasses.map { f => s"${f._1}(${params.paramsCall});" }.mkString(s"${exprFunEnd()}\n")}
             }
             """.code

          ),subClasses.flatMap(_._2))
        }
      funPairs.toSeq
    }.foldLeft((Seq.empty[String], Seq.empty[CodeAndComment])){
      case ((ns, cs), (n, c)) => (ns :+ n, cs ++ c.flatten)
    }

    if (ctx.currentVars eq null) {
      // only fails on "via ProcessFactory with Avro inputs" RowToRowTest shows it doesn't always work for projections

      val subExpressionCode = QualityCodeGenUtils.nonWholeStageSubexpressionElimination(ctx, groupExprs)

      val (funNames, extraClasses) = builder
      (funNames.iterator, subExpressionCode, extraClasses)
    } else {
      // will generate again, the sub exprs will be present on the projection unless ZeroCodeGen is enabled
      val subExprs = SubExprCodeGen.subexpressionEliminationForWholeStageCodegen(ctx, groupExprs ++
        ShimExprUtils.currentSubExprState(ctx).map(s => ShimExprUtils.fromState(s._1))
      )
      val subExpressionCode = ShimExprUtils.evaluateSubExprEliminationState(ctx, subExprs)

      val (funNames, extraClasses) =
        QualityCodeGenUtils.withSubExprEliminationExprs(ctx, subExprs.states) {
          builder
        }
      (funNames.iterator, subExpressionCode, extraClasses)
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