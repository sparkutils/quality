package com.sparkutils.quality.impl

import com.sparkutils.quality.impl.RuleSuiteHelpers.getContextOrSparkClassLoader
import com.sparkutils.quality.impl.util.ExtraConfig.ConfigMapOps
import com.sparkutils.quality.impl.util.TopLevelBooleanSuiteBuilder.triggers
import com.sparkutils.quality.impl.util._
import com.sparkutils.quality.{QualityException, getConfig, groupProcessorKey}
import com.sparkutils.shim.codegen.SubExprCodeGen
import org.apache.spark.sql.ClassicQualitySparkUtils.{genParams, genParamsForNested}
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow}

import scala.runtime.{AbstractFunction10, AbstractFunction9}

sealed trait GroupOr {
  def fold[T](groupsF: Seq[Group] => T)(triggersF: Seq[Trigger] => T): T
  def size: Int
  def groupFilters: Seq[Expression] = Seq.empty
}

case class Triggers(triggers: Seq[Trigger]) extends GroupOr {
  override def fold[T](groupsF: Seq[Group] => T)(triggersF: Seq[Trigger] => T): T = triggersF(triggers)

  override def size: Int = triggers.size
}

case class Groups(groups: Seq[Group]) extends GroupOr {
  override def fold[T](groupsF: Seq[Group] => T)(triggersF: Seq[Trigger] => T): T = groupsF(groups)

  override def size: Int = groups.map(_.size).sum

  override def groupFilters: Seq[Expression] = groups.flatMap(_.groupFilters)
}

case class Trigger(expression: Expression, index: Int, salience: Int, outputExpression: Option[Expression] = None)

case class Group(groupFilter: Expression, lowestSalience: Int, payload: GroupOr) {
  def size = payload.size

  def groupFilters: Seq[Expression] = payload.groupFilters :+ groupFilter
}

case class TriggerResult(groupCalls: Iterator[String], subExpressions: String,
                         extraClasses: Seq[(Int, CodeAndComment)], ignoreTopLevelSubExpressions: Boolean)

/**
 * Allow customised grouping of runner triggers, DQ and ExpressionRunner should evaluate all so the default
 * implementation is sufficient.  This abstraction was added as part of #129 due to 20k trigger rules.
 *
 * The return type is the list of function names to call and any extra common subexpressions needed to group
 */
trait TriggerGrouper extends AbstractFunction10[CodegenContext, Runner, String, Seq[VariableValue],
  Seq[(Trigger, (CodegenContext, ParameterInformation, Expression, Boolean) => Block)], ParameterInformation,
  String, () => Block, () => Block, String => Block, TriggerResult] {

  def apply(ctx: CodegenContext, runner: Runner, resultRow: String, additionalParams: Seq[VariableValue],
            expressions: Seq[(Trigger, (CodegenContext, ParameterInformation, Expression, Boolean) => Block)], params: ParameterInformation,
            prefix: String, exprEnd: () => Block, exprFunEnd: () => Block, groupSalienceCheck: String => Block): TriggerResult

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
                      expressions: Seq[(Trigger, (CodegenContext, ParameterInformation, Expression, Boolean) => Block)],
                      params: ParameterInformation, prefix: String, exprEnd: () => Block, exprFunEnd: () => Block,
                      groupSalienceCheck: String => Block):
    TriggerResult = {

    val allExpr = expressions.grouped(runner.variablesPerFunc).grouped(runner.variableFuncGroup)

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
                QualityCodeGenUtils.splitExpressions(ctx, exprFunc.map( p =>
                  p._2.apply(ctx, params, p._1.expression, false).code + s"${exprEnd()}\n")
                  , runner.variableFuncGroup, exprFuncName, argPairs,
                  foldFunctions =  _.mkString(s"${exprEnd()}\n", s";\n${exprEnd()}\n", ";")
                )
              /*ctx.addNewFunction(exprFuncName,
                code"""
                 private void $exprFuncName(${params.paramsDef}) {
                   $body
                 }
                """.code)*/
              body
            }

          code"""
           private void $groupName(${params.paramsDef}) {
             ${
                //funNames.map { f => s"$f(${params.paramsCall});" }.mkString(s"${exprFunEnd()}\n")
                funNames.mkString(s"${exprFunEnd()}\n")
              }
           }
           """.code

        })
      }
    TriggerResult(funNames, "", Seq.empty[(Int, CodeAndComment)], false)
  }

  def dumpAudit(runner: HasOutput): Unit = {}
}

/**
 * Default grouping approach, implementations may call performGrouping with their own Groups.
 */
trait GroupBasedGrouper extends TriggerGrouper {

  @transient
  private lazy val idHolder = new Counter()

  protected def performGrouping(ctx: CodegenContext, runner: Runner, resultRow: String,
                                additionalParams: Seq[VariableValue],
                                expressions: Seq[(Trigger, (CodegenContext, ParameterInformation, Expression, Boolean) => Block)],
                                params: ParameterInformation, prefix: String, exprEnd: () => Block,
                                exprFunEnd: () => Block, groupSalienceCheck: String => Block,
                                groups: Seq[Group]): TriggerResult = {
    val map = expressions.map(p => p._1.index -> p._2).toMap

    val simpleGrouper = DefaultTriggerGrouper()

    //System.out.println(s"the groups had ${groups.size} entries")

    val groupExprs = groups.flatMap(_.groupFilters)

    def builder = {

      val grouped = groups.grouped(runner.variablesPerFunc).grouped(runner.variableFuncGroup).toSeq

      val funPairs =
        for (exprGroup <- grouped) yield {
          val groupName = ctx.freshName(prefix + "GEGroup")

          val subGroups =
            for {
              exprFunc <- exprGroup
            } yield {
              produceGroups(ctx, runner, additionalParams, prefix, exprEnd, exprFunEnd,
                groupSalienceCheck, 0, simpleGrouper, map, params, exprFunc)
            }

          (ctx.addNewFunction(groupName,

            code"""
             private void $groupName(${params.paramsDef}) {
               ${exprFunEnd()}
               ${subGroups.map { f => s"${f._1}(${params.paramsCall});" }.mkString(s"${exprFunEnd()}\n")}
             }
             """.code

          ), subGroups.flatMap(_._2))
        }
      funPairs
    }.foldLeft((Seq.empty[String], Seq.empty[(Int, CodeAndComment)])) {
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

  protected def produceGroups(ctx: CodegenContext, runner: Runner, additionalParams: Seq[VariableValue], prefix: String,
                              exprEnd: () => Block, exprFunEnd: () => Block, groupSalienceCheck: String => Block,
                              groupDepth: Int, simpleGrouper: DefaultTriggerGrouper,
                              map: Map[Int, (CodegenContext, ParameterInformation, Expression, Boolean) => Block],
                              params: ParameterInformation, exprFunc: Seq[Group]
                             ): (String, Seq[Seq[(Int, CodeAndComment)]]) = {
    val exprFuncName = ctx.freshName(prefix + "GEFuncGroup" + groupDepth)

    val argPairs = params.nonCombinedParams.map(t => t._1 -> t._2)
    val groupCalls =
      exprFunc.map {
        group =>

          producePayload(ctx, runner, additionalParams, prefix, exprEnd, exprFunEnd,
            groupSalienceCheck, group, simpleGrouper, map, groupDepth, params)
      }

    val body =
      QualityCodeGenUtils.splitExpressions(ctx, groupCalls.map(_._1 + s"${exprEnd()}\n"),
        runner.variablesPerFunc, exprFuncName, argPairs,
        foldFunctions = _.mkString(s"${exprEnd()}\n", s";\n${exprEnd()}\n", ";")
      )

    (ctx.addNewFunction(exprFuncName,
      code"""
       private void $exprFuncName(${params.paramsDef}) {
         $body
       }
      """.code
    ), groupCalls.map(_._2))
  }

  protected def producePayload(ctx: CodegenContext, runner: Runner, additionalParams: Seq[VariableValue], prefix: String,
                              exprEnd: () => Block, exprFunEnd: () => Block, groupSalienceCheck: String => Block,
                              group: Group, simpleGrouper: DefaultTriggerGrouper,
                              map: Map[Int, (CodegenContext, ParameterInformation, Expression, Boolean) => Block],
                              groupDepth: Int, params: ParameterInformation): (String, Seq[(Int, CodeAndComment)]) = {
    group.payload.fold { groups =>
      val (funName, clazzes) =
        produceGroups(ctx, runner, additionalParams, prefix, exprEnd, exprFunEnd,
          groupSalienceCheck, groupDepth + 1, simpleGrouper, map, params, groups)

      val eval = group.groupFilter.genCode(ctx)

      // if ruleEngine is used salience may need comparison, if it's expression or dq any comparison is meaningless
      (code"""
        ${exprEnd()}\n
        ${eval.code}
        if ((!${eval.isNull}) && ${eval.value} ${groupSalienceCheck(group.lowestSalience.toString)} ) {
          $funName(${params.paramsCall});
        }
      """.code, clazzes.flatten)
    } { triggers =>
      val (body, clazzes) =
        produceGroupTriggers(ctx, runner, additionalParams, prefix, exprEnd, exprFunEnd,
          groupSalienceCheck, group, simpleGrouper, map, triggers)
      (body.code, clazzes)
    }
  }

  protected def produceGroupTriggers(ctx: CodegenContext, runner: Runner, additionalParams: Seq[VariableValue], prefix: String,
                                     exprEnd: () => Block, exprFunEnd: () => Block, groupSalienceCheck: String => Block,
                                     group: Group, simpleGrouper: DefaultTriggerGrouper,
                                     map: Map[Int, (CodegenContext, ParameterInformation, Expression, Boolean) => Block],
                                     triggers: Seq[Trigger]): (Block, Seq[(Int, CodeAndComment)]) = {
    val groupIndex = idHolder.next()
    val id = s"Group$groupIndex"

    val allGroupExprs = triggers.flatMap {
      trigger =>
        Seq(trigger.expression) ++ trigger.outputExpression.map(Seq(_)).getOrElse(Seq.empty)
    }

    // remove the params usage, everything is in the object variables, this is top level only
    val preCalcParams = (ctx: CodegenContext) =>
      genParamsForNested(ctx, allGroupExprs :+ group.groupFilter, additionalParams).copy(paramsDef = "", paramsCall = "")

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

        val gr =
          SwitchGroups.groups(triggers).map(_.produceGroup(ctx, prefix, 0, map, params)).getOrElse{
            simpleGrouper(ctx, runner, grpResult, additionalParams,
              triggers.map(t => (t, map(t.index))), params, prefix, exprEnd, exprFunEnd, groupSalienceCheck)
          }

        val funNames = gr.groupCalls
        val exprRunner =
          ExprCode(VariableValue(ctx.freshName("groupResultNull"), java.lang.Boolean.TYPE),
            VariableValue(grpResult, classOf[Array[Object]])
          )

        val noArrayParams = additionalParams.filterNot(_.javaType.isArray);

        val returnArray = ctx.addMutableState("Object[]","returnArray",
          initFunc = v => s"$v = new Object[${noArrayParams.size}];")

        // the top level is 0 arrays are filtered out from the row as they don't need explicit returning
        GenerateResult(SeparateClassGenerator(runner.getClass.getName, additionalParams, groupIndex + 1), exprRunner.copy(
          code =
            code"""
              boolean ${exprRunner.isNull} = false;
              ${funNames.map { f => s"$f(${params.paramsCall});" }.mkString("\n")}
              Object[] ${exprRunner.value} = $returnArray;
              ${noArrayParams.zipWithIndex.map{
                case (p, index) => s"${returnArray}[$index] = ${p.variableName};"
              }.mkString(";\n")}
              """
        ), gr.extraClasses, gr.ignoreTopLevelSubExpressions)
      }

    val eval = group.groupFilter.genCode(ctx)

    // if ruleEngine is used salience may need comparison, if it's expression or dq any comparison is meaningless
    (
      code"""
        ${exprEnd()}\n
        ${eval.code}
        if ((!${eval.isNull}) && ${eval.value} ${groupSalienceCheck(group.lowestSalience.toString)} ) {
          ${expr.code}
        }
      """, body)
  }
}

/**
 * Groups by common top level Boolean And and EqualTo expressions with Literals, using buckets of hashes on the literal
 * values.  Using this approach can lead to a 10x spread increase over the default grouper for very large truth table
 * style rules (tested against the 20k_rule_suite.csv in the BigRules testsuite).
 *
 * Only supported with Spark 3.2 and above
 */
case class TopLevelBooleanGrouper() extends GroupBasedGrouper {

  override def apply( ctx: CodegenContext, runner: Runner, resultRow: String, additionalParams: Seq[VariableValue],
                      expressions: Seq[(Trigger, (CodegenContext, ParameterInformation, Expression, Boolean) => Block)],
                      params: ParameterInformation, prefix: String, exprEnd: () => Block, exprFunEnd: () => Block,
                      groupSalienceCheck: String => Block):
    TriggerResult = {

    val targetParams = TopLevelBoolean.params(runner)

    val groups = TopLevelBoolean.bucket(expressions.map(_._1), targetParams)

    performGrouping(ctx, runner, resultRow, additionalParams, expressions, params, prefix, exprEnd,
      exprFunEnd, groupSalienceCheck, groups)
  }

  override def dumpAudit(runner: HasOutput): Unit = {
    TopLevelBooleanSuiteBuilder.build(runner)
    val (_,size) = TopLevelBoolean.bestFit(triggers(runner))
    System.out.println(s"TopLevelBooleanGrouper - optimal size between 100 and 200 for ruleSuite ${runner.ruleSuite.id} is $size")
  }

}

object Triggers {

  def loadTriggerGrouper(extraConfig: Map[String, String]): TriggerGrouper = {
    val name = extraConfig.string(groupProcessorKey, classOf[DefaultTriggerGrouper].getName)

    val impl =
      try {
        Class.forName(name, false, getContextOrSparkClassLoader).newInstance().asInstanceOf[TriggerGrouper]
      } catch {
        case t: Throwable => throw QualityException(s"Could not load TriggerGrouper of name $name", t)
      }
    impl
  }

}