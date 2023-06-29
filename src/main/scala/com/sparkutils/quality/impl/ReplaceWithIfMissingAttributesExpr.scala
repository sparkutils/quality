package com.sparkutils.quality.impl

import com.sparkutils.quality.RuleLogicUtils.mapRules
import com.sparkutils.quality.VariablesLookup.{fieldsFromExpression, toName}
import com.sparkutils.quality.impl.imports.RuleRunnerImports
import com.sparkutils.quality.utils.LookupIdFunctions
import com.sparkutils.quality.{ExpressionRule, ExpressionRuleExpr, LambdaFunction, LambdaFunctionParsed, OutputExpression, OutputExpressionExpr, Rule, RuleLogicUtils, RuleSuite, RunOnPassProcessorImpl, namesFromSchema}
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, LambdaFunction => SparkLambdaFunction}
import org.apache.spark.sql.types.{NullType, StructType}



object ProcessDisableIfMissing extends RuleRunnerImports {
  /**
   * Processes a given RuleSuite to replace any coalesceIfMissingAttributes.  This may be called before validate / docs but
   * *must* be called *before* adding the expression to a dataframe.
   *
   * @param ruleSuite
   * @param schema The names to validate against, if empty no attempt to process coalesceIfAttributeMissing will be made
   * @return
   */
  def processIfAttributeMissing(ruleSuite: RuleSuite, schema: StructType = StructType(Seq())) = {
    val names = LookupIdFunctions.namesFromSchema(schema)

    val lambdas = ruleSuite.lambdaFunctions.map(lf => ProcessDisableIfMissing.processIfMissingLambdaCoalesce(lf.parsed, names))

    val wrs = ruleSuite.copy(lambdaFunctions = lambdas)

    mapRules(wrs) {
      ProcessDisableIfMissing.processCoalesceIfAttributeMissing(_, names)
    }
  }


  /**
   * For a given schema evaluates all calls to coalesce in a lambda to swap out missing attributes
   *
   * Must be called before any planning - otherwise Spark will already have rejected it.
   *
   * @param rule
   * @return
   */
  def processIfMissingLambdaCoalesce(rule: LambdaFunctionParsed, names: Set[String]): LambdaFunction =
    rule.expr match {
      case lambdaFunc: SparkLambdaFunction =>
        lambdaFunc.function match {
          case func: UnresolvedFunction =>
            val rexp = processCoalesceIfAttributeMissing(func, names)
            rule.copy(expr = lambdaFunc.copy(function = rexp))

          case _ => rule
        }
    }

  protected def isReplaceCoalesceName(unresolvedFunction: UnresolvedFunction) = {
    val name = toName(unresolvedFunction)
    name == "coalesceIfAttributesMissing" || name == "coalesceIfAttributesMissingDisable"
  }

  protected def isCoalesceDisabled(unresolvedFunction: UnresolvedFunction) = {
    val name = toName(unresolvedFunction)
    name == "coalesceIfAttributesMissingDisable"
  }

  /**
   * Converts expression removing any coalesceIfAttributeMissing placeholding expressions using names to find each
   * first expression with no missing names (or replacing with null should all of them be).
   * @param expression
   * @param names
   * @return
   */
  def processCoalesceIfAttributeMissing(expression: Expression, names: Set[String]): Expression =
    expression match {
      case funcExpr: UnresolvedFunction if isReplaceCoalesceName(funcExpr) =>
        processCoalesceIfAttributeMissing(expression.children, names, isCoalesceDisabled(funcExpr))
      case e: Expression =>
        e.withNewChildren(e.children.map(processCoalesceIfAttributeMissing(_, names)))
    }

  /**
   * Returns the first expression that has no missing names or null
   * @param expressions
   * @param names
   * @return
   */
  protected def processCoalesceIfAttributeMissing(expressions: Seq[Expression], names: Set[String], isDisabled: Boolean): Expression = {
    val res =
      expressions.flatMap{ e =>
        // make sure no nested children are coalesce's
        val r = processCoalesceIfAttributeMissing(e, names)
        // get the names from this expression
        val explookup = fieldsFromExpression(r)
        // lookup each of them in the names and if any are not true we can't use this expression
        if (!explookup.attributesUsed.map(names).forall(identity))
          None
        else
          Some(r)
      }

    if (res.isEmpty) {
      if (isDisabled)
        DisabledRuleExpr // special case as this is probably before the function registry gets called
      else
        Literal(null, NullType)
    } else
      res.head
  }

  /**
   * Converts both rule expressions and any output expressions removing any coalesceIfAttributeMissing placeholding expressions using names to find each
   * first expression with no missing names (or replacing with null should all of them be).
   * @param names
   * @return
   */
  protected[quality] def processCoalesceIfAttributeMissing(rule: Rule, names: Set[String]): Rule =
    rule match {
      // rule and output
      case Rule(id, ExpressionRule(rule: String),
        iorule @ RunOnPassProcessorImpl(_, _, _, OutputExpression(oruleExpr: String))) if oruleExpr.nonEmpty =>
        Rule(id, ExpressionRuleExpr(rule, processCoalesceIfAttributeMissing(RuleLogicUtils.expr(rule), names)),
          iorule.copy(returnIfPassed = OutputExpressionExpr(oruleExpr,
            processCoalesceIfAttributeMissing(RuleLogicUtils.expr(oruleExpr), names))))

      // just a rule
      case orule @ Rule(_, ExpressionRule(rule: String), _) =>
        orule.copy( expression = ExpressionRuleExpr(rule, processCoalesceIfAttributeMissing(RuleLogicUtils.expr(rule), names)))

      case _ => rule
    }
}