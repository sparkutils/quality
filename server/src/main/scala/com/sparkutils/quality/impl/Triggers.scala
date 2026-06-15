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
import scala.util.Try

trait GroupLike {
  def lowestSalience: Int
  def payload: GroupOr
  def size = payload.size

  def groupExpression: Expression

  def groupFilters: Seq[Expression] = payload.groupFilters

  def evalValueCheck(eval: ExprCode) = s" && ${eval.value}"


  def call(ctx: CodegenContext, expr: ExprCode, exprEnd: () => Block, groupSalienceCheck: String => Block,
           params: ParameterInformation): Block = {
    val eval = groupExpression.genCode(ctx)
    val evalValue = evalValueCheck(eval)

    // complexity of the eval and expr.code may blow JIT limits on the group call, split to allow jvm to inline if needed
    val exprFuncName = ctx.freshName("GEGroupCall")
    val fun = ctx.addNewFunction(exprFuncName,
      code"""
       private void $exprFuncName(${params.paramsDef}) {
         ${exprEnd()}\n
         ${eval.code}
         if ((!${eval.isNull}) $evalValue ${groupSalienceCheck(lowestSalience.toString)} ) {
           ${expr.code}
         }
       }
      """.code
    )
    code"""
    $fun(${params.paramsCall});
    """
  }
}

trait GroupOr {

  def fold[T](groupsF: Seq[GroupLike] => T)(triggersF: Seq[Trigger] => T): T
  def size: Int
  def groupFilters: Seq[Expression] = Seq.empty

  def producePayload(ctx: CodegenContext, runner: Runner, additionalParams: Seq[VariableValue], prefix: String,
                               exprEnd: () => Block, exprFunEnd: () => Block, groupSalienceCheck: String => Block,
                               group: GroupLike, simpleGrouper: DefaultTriggerGrouper,
                               map: Map[Int, (CodegenContext, ParameterInformation, Expression, Boolean) => Block],
                               groupDepth: Int, params: ParameterInformation, idHolder: Counter): (String, Seq[(Int, CodeAndComment)])

  def produceGroups(ctx: CodegenContext, runner: Runner, additionalParams: Seq[VariableValue], prefix: String,
                              exprEnd: () => Block, exprFunEnd: () => Block, groupSalienceCheck: String => Block,
                              groupDepth: Int, simpleGrouper: DefaultTriggerGrouper,
                              map: Map[Int, (CodegenContext, ParameterInformation, Expression, Boolean) => Block],
                              params: ParameterInformation, exprFunc: Seq[GroupLike], idHolder: Counter
                             ): (String, Seq[Seq[(Int, CodeAndComment)]]) = {
    val exprFuncName = ctx.freshName(prefix + "GEFuncGroup" + groupDepth)

    val argPairs = params.nonCombinedParams.map(t => t._1 -> t._2)
    val groupCalls =
      exprFunc.map {
        group =>

          group.payload.producePayload(ctx, runner, additionalParams, prefix, exprEnd, exprFunEnd,
            groupSalienceCheck, group, simpleGrouper, map, groupDepth, params, idHolder)
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

  protected def produceGroupTriggers(ctx: CodegenContext, runner: Runner, additionalParams: Seq[VariableValue], prefix: String,
                                     exprEnd: () => Block, exprFunEnd: () => Block, groupSalienceCheck: String => Block,
                                     group: GroupLike, simpleGrouper: DefaultTriggerGrouper,
                                     map: Map[Int, (CodegenContext, ParameterInformation, Expression, Boolean) => Block],
                                     triggers: Seq[Trigger], idHolder: Counter, params: ParameterInformation): (Block, Seq[(Int, CodeAndComment)]) = {
    val allGroupExprs = triggers.flatMap {
      trigger =>
        Seq(trigger.expression) ++ trigger.outputExpression.map(Seq(_)).getOrElse(Seq.empty)
    }

    separateCompilation(allGroupExprs: Seq[Expression], ctx: CodegenContext, runner: Runner, additionalParams: Seq[VariableValue],
      exprEnd: () => Block, groupSalienceCheck: String => Block,
      group.groupFilters, idHolder: Counter, params: ParameterInformation) { (ctx, index, params, groupIndex) =>
        // group the group, params holds any subexprs used/generated for this sub compilation

        val grpResult = ctx.freshName("groupResult")

        val sgr = simpleGrouper(ctx, runner, grpResult, additionalParams,
          triggers.map(t => (t, map(t.index))), params, prefix, exprEnd, exprFunEnd, groupSalienceCheck)

        val funNames = sgr.groupCalls
        val exprRunner =
          ExprCode(VariableValue(ctx.freshName("groupResultNull"), java.lang.Boolean.TYPE),
            VariableValue(grpResult, classOf[GenericInternalRow])
          )

        // the top level is 0 arrays are filtered out from the row as they don't need explicit returning
        GenerateResult(SeparateClassGenerator(runner.getClass.getName, additionalParams, groupIndex + 1), exprRunner.copy(
          code =
            code"""
              boolean ${exprRunner.isNull} = false;
              ${funNames.map { f => s"$f(${params.paramsCall});" }.mkString("\n")}
              GenericInternalRow ${exprRunner.value} = new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(
                new Object[]{
                ${additionalParams.filterNot(_.javaType.isArray).map(_.variableName).mkString(",\n")}
                }
              );
              """
        ), sgr.extraClasses, sgr.ignoreTopLevelSubExpressions, groupIndex + 1)
      } { (ctx, expr) =>
      group.call(ctx, expr, exprEnd, groupSalienceCheck, params)
    }
  }

  protected def separateCompilation(allGroupExprs: Seq[Expression], ctx: CodegenContext, runner: Runner, additionalParams: Seq[VariableValue],
                                     exprEnd: () => Block, groupSalienceCheck: String => Block,
                                    top: Seq[Expression], idHolder: Counter, params: ParameterInformation)(
                                   compile: (CodegenContext, Int, ParameterInformation, Int) => GenerateResult[SeparateClassGenerator]
      )(call: (CodegenContext, ExprCode) => Block): (Block, Seq[(Int, CodeAndComment)]) = {
    val groupIndex = idHolder.next()
    val id = s"Group$groupIndex"

    // remove the params usage, everything is in the object variables, this is top level only
    val preCalcParams = (ctx: CodegenContext) =>
      genParamsForNested(ctx, allGroupExprs ++ top, additionalParams).copy(paramsDef = "", paramsCall = "")

    val resCode = ExprCode(VariableValue(ctx.freshName("groupResultNull"), java.lang.Boolean.TYPE),
      VariableValue(ctx.freshName("groupResult"), classOf[GenericInternalRow]))
    val (body, expr) =
      SeparateCompilation.withSubExpressions(runner, allGroupExprs, ctx, resCode,
        SubCompilation(id, s"Trigger group $groupIndex"),
        // we need to pipe the row in
        extraParams = additionalParams,
        useParams = preCalcParams
      ) (compile(_,_,_,groupIndex))

    // if ruleEngine is used salience may need comparison, if it's expression or dq any comparison is meaningless
    (call(ctx, expr), body)
  }
}

case class Triggers(triggers: Seq[Trigger]) extends GroupOr {
  override def fold[T](groupsF: Seq[GroupLike] => T)(triggersF: Seq[Trigger] => T): T = triggersF(triggers)

  override def size: Int = triggers.size

  def producePayload(ctx: CodegenContext, runner: Runner, additionalParams: Seq[VariableValue], prefix: String,
                               exprEnd: () => Block, exprFunEnd: () => Block, groupSalienceCheck: String => Block,
                               group: GroupLike, simpleGrouper: DefaultTriggerGrouper,
                               map: Map[Int, (CodegenContext, ParameterInformation, Expression, Boolean) => Block],
                               groupDepth: Int, params: ParameterInformation, idHolder: Counter): (String, Seq[(Int, CodeAndComment)]) = {
    val (body, clazzes) =
      produceGroupTriggers(ctx, runner, additionalParams, prefix, exprEnd, exprFunEnd,
        groupSalienceCheck, group, simpleGrouper, map, triggers, idHolder, params)
    (body.code, clazzes)
  }

}

class GroupsBase(groups: Seq[GroupLike]) extends GroupOr {
  override def fold[T](groupsF: Seq[GroupLike] => T)(triggersF: Seq[Trigger] => T): T = groupsF(groups)

  override def size: Int = groups.map(_.size).sum

  override def groupFilters: Seq[Expression] = groups.flatMap(_.groupFilters)

  def producePayload(ctx: CodegenContext, runner: Runner, additionalParams: Seq[VariableValue], prefix: String,
                               exprEnd: () => Block, exprFunEnd: () => Block, groupSalienceCheck: String => Block,
                               group: GroupLike, simpleGrouper: DefaultTriggerGrouper,
                               map: Map[Int, (CodegenContext, ParameterInformation, Expression, Boolean) => Block],
                               groupDepth: Int, params: ParameterInformation, idHolder: Counter): (String, Seq[(Int, CodeAndComment)]) = {
    val (funName, clazzes) =
      produceGroups(ctx, runner, additionalParams, prefix, exprEnd, exprFunEnd,
        groupSalienceCheck, groupDepth + 1, simpleGrouper, map, params, groups, idHolder)
    (s"$funName(${params.paramsCall});", clazzes.flatten)
  }

}

case class Groups(groups: Seq[GroupLike]) extends GroupsBase(groups) {

}

/**
 * Produces grouping via a switch, unlike normal grouping the group itself is the separate compilation and
 * each sub SwitchGroup is directly inlined if there is only one trigger per group
 * @param groups
 */
case class SwitchGroups(groups: Seq[GroupLike], groupingExpression: Expression, typ: String,
                        conversion: String => String = identity) extends GroupsBase(groups) {

  override def produceGroups(ctx: CodegenContext, runner: Runner, additionalParams: Seq[VariableValue], prefix: String,
                    exprEnd: () => Block, exprFunEnd: () => Block, groupSalienceCheck: String => Block,
                    groupDepth: Int, simpleGrouper: DefaultTriggerGrouper,
                    map: Map[Int, (CodegenContext, ParameterInformation, Expression, Boolean) => Block],
                    params: ParameterInformation, exprFunc: Seq[GroupLike], idHolder: Counter
                   ): (String, Seq[Seq[(Int, CodeAndComment)]]) = {

    require(exprFunc.forall(_.isInstanceOf[SwitchGroup[_]]), "All SwitchGroups groups must be of type SwitchGroup")

    val inline = Try{exprFunc.forall(_.asInstanceOf[SwitchGroup[_]].payload.asInstanceOf[Triggers].triggers.size == 1)}.
      getOrElse(false)

    if (!inline) {

      val groupCalls: Seq[(String, (String, Seq[(Int, CodeAndComment)]))] =
        exprFunc.map {
          group =>
            val sg = group.asInstanceOf[SwitchGroup[_]]
            sg.bucket ->
              sg.payload.producePayload(ctx, runner, additionalParams, prefix, exprEnd, exprFunEnd,
                groupSalienceCheck, group, simpleGrouper, map, groupDepth, params, idHolder)
        }

      generateSwitch(ctx, prefix, groupDepth, params, groupCalls)
    } else {
      val triggers = exprFunc.map(_.asInstanceOf[SwitchGroup[_]].payload.asInstanceOf[Triggers])
      val allGroupExprs = triggers.flatMap {
        triggers => triggers.triggers.flatMap {
          trigger =>
            Seq(trigger.expression) ++ trigger.outputExpression.map(Seq(_)).getOrElse(Seq.empty)
        }
      }

      val (b, single) =
        separateCompilation(allGroupExprs: Seq[Expression], ctx: CodegenContext, runner: Runner, additionalParams: Seq[VariableValue],
          exprEnd: () => Block, groupSalienceCheck: String => Block,
          Seq(groupingExpression), idHolder: Counter, params: ParameterInformation) { (ctx, index, params, groupIndex) =>
          // group the group, params holds any subexprs used/generated for this sub compilation

          val grpResult = ctx.freshName("groupResult")

          val groupCalls: Seq[(String, (String, Seq[(Int, CodeAndComment)]))] =
            exprFunc.map {
              group =>
                val sg = group.asInstanceOf[SwitchGroup[_]]

                val trigger = sg.payload.asInstanceOf[Triggers].triggers.map(t => (t, map(t.index))).head
                val code = trigger._2(ctx, params, trigger._1.expression, true) // alreadyPassed, so don't generate test

                sg.bucket -> (code.code, Seq.empty)

                /*sg.payload.producePayload(ctx, runner, additionalParams, prefix, exprEnd, exprFunEnd,
                    groupSalienceCheck, group, simpleGrouper, map, groupDepth, params, idHolder)*/
            }

          val (switchFunName, codes) = generateSwitch(ctx, prefix, groupDepth, params, groupCalls)

          val exprRunner =
            ExprCode(VariableValue(ctx.freshName("groupResultNull"), java.lang.Boolean.TYPE),
              VariableValue(grpResult, classOf[GenericInternalRow])
            )

          // the top level is 0 arrays are filtered out from the row as they don't need explicit returning
          GenerateResult(SeparateClassGenerator(runner.getClass.getName, additionalParams, groupIndex + 1), exprRunner.copy(
            code =
              code"""
                boolean ${exprRunner.isNull} = false;
                $switchFunName(${params.paramsCall});
                GenericInternalRow ${exprRunner.value} = new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(
                  new Object[]{
                  ${additionalParams.filterNot(_.javaType.isArray).map(_.variableName).mkString(",\n")}
                  }
                );
                """
          ), groupCalls.flatMap(_._2._2) ++ codes.flatten, true, groupIndex + 1)
        } { (ctx, expr) =>
          expr.code
        }

      val exprFuncName = ctx.freshName("GESwitchGroupCall")
      val fun = ctx.addNewFunction(exprFuncName,
        code"""
         private void $exprFuncName(${params.paramsDef}) {
             ${b.code}
         }
        """.code
      )
      (fun, Seq(single))
    }
  }

  private def generateSwitch(ctx: CodegenContext, prefix: String, groupDepth: Int, params: ParameterInformation,
                             groupCalls: Seq[(String, (String, Seq[(Int, CodeAndComment)]))]) = {
    val exprFuncName = ctx.freshName(prefix + "GEFuncSwitchGroups" + groupDepth)

    val cases = groupCalls.map {
      case (bucket, (codeToRun, _)) =>
        val exprFuncName = ctx.freshName(prefix + "callGEFuncSwitchGroup" + groupDepth)
        val callFun = ctx.addNewFunction(exprFuncName,
          code"""
           private void $exprFuncName(${params.paramsDef}) {
             $codeToRun
           }
          """.code
        )
        code"""
          case $bucket:
            $callFun(${params.paramsCall});
            break;
        """
    }

    val expr = groupingExpression.genCode(ctx)

    def buildSwitch(switchVal: String, cases: Seq[Block], default: String): String = {
      val folded = cases.foldLeft(code"") {
        case (cur, n) =>
          code"""
          $cur
          $n
        """
      }
      s"""
        switch($switchVal) {
          $folded
          ${
        if (default.isEmpty) ""
        else
          s"""
          default:
            $default
            """
      }
        }
         """
    }

    def buildSwitches(switchVal: String, chunked: Seq[Seq[Block]]): String = {
      if (chunked.size == 1) {
        buildSwitch(switchVal, chunked.head, "")
      } else {
        // we have more chunks
        val head = chunked.head
        val switch = buildSwitches(switchVal, chunked.tail)

        val exprFuncName = ctx.freshName(prefix + s"GEFuncSwitchGroupsNested_" + groupDepth)
        val callFun = ctx.addNewFunction(exprFuncName,
          code"""
           private void $exprFuncName($typ $switchVal, ${params.paramsDef}) {
             $switch
           }
          """.code
        )

        buildSwitch(switchVal, head, s"$callFun($switchVal, ${params.paramsCall});")
      }
    }

    val converted = conversion(expr.value)
    val (converting, switchName) =
      if (converted.isEmpty || converted == expr.value.code)
        ("", expr.value.code)
      else {
        val fresh = ctx.freshName("converted")
        (s"$typ $fresh = $converted;", fresh)
      }

    (ctx.addNewFunction(exprFuncName,
      code"""
       private void $exprFuncName(${params.paramsDef}) {
         ${expr.code}
         $converting
         if (${expr.isNull}) {} else {
           ${
        buildSwitches(switchName, /// TODO uses Spark mechanism to group, this is for PoC
          cases.grouped(200).toSeq)
      }
         }
       }
      """.code
    ), groupCalls.map(_._2._2))
  }
}

case class Trigger(expression: Expression, index: Int, salience: Int, outputExpression: Option[Expression] = None)

case class Group(groupFilter: Expression, lowestSalience: Int, payload: GroupOr) extends GroupLike {

  override def groupFilters: Seq[Expression] = payload.groupFilters :+ groupFilter

  def groupExpression = groupFilter
}

case class SwitchGroup[T](bucket: String, lowestSalience: Int, payload: GroupOr, bucketRaw: T) extends GroupLike {

  def groupExpression: Expression = ???

  override def evalValueCheck(eval: ExprCode): String = "" // no test needed other than null

  override def call(ctx: CodegenContext, expr: ExprCode, exprEnd: () => Block, groupSalienceCheck: String => Block,
                    params: ParameterInformation): Block = {
    // if ruleEngine is used salience may need comparison, if it's expression or dq any comparison is meaningless
    code"""
      ${exprEnd()}\n
      if ( true ${groupSalienceCheck(lowestSalience.toString)} ) {
        ${expr.code}
      }
    """
  }

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
              Groups(exprFunc).produceGroups(ctx, runner, additionalParams, prefix, exprEnd, exprFunEnd,
                groupSalienceCheck, 0, simpleGrouper, map, params, exprFunc, idHolder)
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