package com.sparkutils.quality.impl

import com.sparkutils.quality.impl.RuleSuiteHelpers.getContextOrSparkClassLoader
import com.sparkutils.quality.impl.util.TopLevelBooleanSuiteBuilder.triggers
import com.sparkutils.quality.impl.util._
import com.sparkutils.quality.{QualityException, getConfig, groupProcessorKey}
import com.sparkutils.shim.codegen.SubExprCodeGen
import org.apache.spark.sql.ClassicQualitySparkUtils.genParamsForNested
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow}

import scala.runtime.AbstractFunction9

case class Trigger(expression: Expression, index: Int, salience: Int, outputExpression: Option[Expression] = None)

case class Group(groupFilter: Expression, lowestSalience: Int, triggers: Seq[Trigger])

case class TriggerResult(groupCalls: Iterator[String], subExpressions: String,
                         extraClasses: Seq[CodeAndComment], ignoreTopLevelSubExpressions: Boolean)

/**
 * Allow customised grouping of runner triggers, DQ and ExpressionRunner should evaluate all so the default
 * implementation is sufficient.  This abstraction was added as part of #129 due to 20k trigger rules.
 *
 * The return type is the list of function names to call and any extra common subexpressions needed to group
 */
trait TriggerGrouper extends AbstractFunction9[CodegenContext, Runner, String, Seq[VariableValue],
  Seq[(Trigger, (CodegenContext, ParameterInformation) => Block)], ParameterInformation,
  String, () => Block, () => Block, TriggerResult] {

  def apply(ctx: CodegenContext, runner: Runner, resultRow: String, additionalParams: Seq[VariableValue],
            expressions: Seq[(Trigger, (CodegenContext, ParameterInformation) => Block)], params: ParameterInformation,
            prefix: String, exprEnd: () => Block, exprFunEnd: () => Block): TriggerResult

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
                      params: ParameterInformation, prefix: String, exprEnd: () => Block, exprFunEnd: () => Block):
    TriggerResult = {

    val allExpr = expressions.map(_._2).grouped(runner.variablesPerFunc).grouped(runner.variableFuncGroup)

    val funNames =
      for (exprGroup <- allExpr) yield {
        val groupName = ctx.freshName(prefix+"EGroup")
        ctx.addNewFunction(groupName, {
          val funNames =
            for {
              exprFunc <- exprGroup
            } yield {
              val exprFuncName = ctx.freshName(prefix+"EFuncGroup")
              val argPairs = params.nonCombinedParams.map(t => t._1 -> t._2)
              val body =
                QualityCodeGenUtils.splitExpressions(ctx, exprFunc.map(_.apply(ctx, params).code + s"${exprEnd()}\n"), exprFuncName, argPairs,
                  foldFunctions =  _.mkString(s"${exprEnd()}\n", s";\n${exprEnd()}\n", ";")
                )
              ctx.addNewFunction(exprFuncName,
                code"""
   private void $exprFuncName(${params.paramsDef}) {
     $body
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
    TriggerResult(funNames, "", Seq.empty[CodeAndComment], false)
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
                      params: ParameterInformation, prefix: String, exprEnd: () => Block, exprFunEnd: () => Block):
    TriggerResult = {

    val targetBucket = TopLevelBoolean.params(runner)

    val map = expressions.toMap

    val groups = TopLevelBoolean.bucket(expressions.map(_._1), targetBucket)
    val simpleGrouper = DefaultTriggerGrouper()

    //System.out.println(s"the groups had ${groups.size} entries")

    val groupExprs = groups.map(_.groupFilter)

    def builder = {

      val grouped = groups.zipWithIndex.grouped(runner.variablesPerFunc).grouped(runner.variableFuncGroup).toSeq

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

                    val allGroupExprs = group.triggers.flatMap{
                      trigger =>
                        Seq(trigger.expression) ++ trigger.outputExpression.map(Seq(_)).getOrElse(Seq.empty)
                    }

                    // remove the params usage, everything is in the object variables, this is top level only
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
                        // group the group, params holds any subexprs used/generated for this sub compilation

                        val grpResult = ctx.freshName("groupResult")

                        val sgr = simpleGrouper(ctx, runner, grpResult, additionalParams,
                          group.triggers.map(t => (t, map(t))), params, prefix, exprEnd, exprFunEnd)

                        val funNames = sgr.groupCalls
                        val exprRunner =
                          ExprCode(VariableValue(ctx.freshName("groupResultNull"), java.lang.Boolean.TYPE),
                            VariableValue(grpResult, classOf[GenericInternalRow])
                          )

                        // the top level is 0 arrays are filtered out from the row as they don't need explicit returning
                        GenerateResult(SeparateClassGenerator(runner.getClass.getName, additionalParams, groupIndex + 1), exprRunner.copy(
                          code = code"""
                              boolean ${exprRunner.isNull} = false;
                              ${funNames.map { f => s"$f(${params.paramsCall});" }.mkString("\n")}
                              GenericInternalRow ${exprRunner.value} = new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(
                                new Object[]{
                                ${additionalParams.filterNot(_.javaType.isArray).map(_.variableName).mkString(",\n")}
                                }
                              );
                              """
                        ), sgr.extraClasses, sgr.ignoreTopLevelSubExpressions)
                      }

                    val eval = group.groupFilter.genCode(ctx)
                    /*
                    //if (groupIndex > 40 && groupIndex < 45) {
                      System.out.println(s"""${s"$id - Size ${group.triggers.size} filter is ${group.groupFilter.toString.replaceAll("\n","")}"}""")
                    //}
                    // dump some samples of the 16k population on databricks
                    if (groupIndex == 43 && groups.size == 43) {
                      System.out.println("dumping every 1k of Group 43's 'true'")
                      (0 until 16).foreach(i => System.out.println(group.triggers(i * 1000).expression.toString()))
                    }*/

                    (code"""
                        ${exprEnd()}\n
                        ${eval.code}
                        if ((!${eval.isNull}) && ${eval.value}  ) {
                          ${expr.code}
                        }
                      """, body)
                }

              val argPairs = params.nonCombinedParams.map(t => t._1 -> t._2)
              val body =
                QualityCodeGenUtils.splitExpressions(ctx, groupCalls.map(_._1.code + s"${exprEnd()}\n"), exprFuncName, argPairs,
                  foldFunctions =  _.mkString(s"${exprEnd()}\n", s";\n${exprEnd()}\n", ";")
                )
//${groupCalls.map(_._1).mkString(s"\n")}
              (ctx.addNewFunction(exprFuncName,
                code"""
                 private void $exprFuncName(${params.paramsDef}) {
                   $body
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
      funPairs
    }.foldLeft((Seq.empty[String], Seq.empty[CodeAndComment])){
      case ((ns, cs), (n, c)) => (ns :+ n, cs ++ c.flatten)
    }

    if (ctx.currentVars eq null) {
      // only fails on "via ProcessFactory with Avro inputs" RowToRowTest shows it doesn't always work for projections

      val subExpressionCode = QualityCodeGenUtils.nonWholeStageSubexpressionElimination(ctx, groupExprs)

      val (funNames, extraClasses) = builder
      TriggerResult(funNames.iterator, subExpressionCode, extraClasses, true)
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
      TriggerResult(funNames.iterator, SeparateCompilation.splitGlobalSubExprs(ctx, subExpressionCode), extraClasses, true)
    }
  }

  override def dumpAudit(runner: HasOutput): Unit = {
    TopLevelBooleanSuiteBuilder.build(runner)
    val (_,size) = TopLevelBoolean.bestFit(triggers(runner))
    System.out.println(s"TopLevelBooleanGrouper - optimal size between 100 and 200 for ruleSuite ${runner.ruleSuite.id} is $size")
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