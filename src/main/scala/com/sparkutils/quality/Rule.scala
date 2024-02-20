package com.sparkutils.quality

import com.sparkutils.quality.impl._
import com.sparkutils.quality.impl.util.VariablesLookup
import com.sparkutils.shim.expressions.Names.toName
import org.apache.spark.internal.Logging
import org.apache.spark.sql.QualitySparkUtils
import org.apache.spark.sql.ShimUtils.arguments
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction}
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Literal}

/**
  * A versioned rule ID - note the name is never persisted in results, the id and version are sufficient to retrieve the name
  * @param id a unique ID to identify this rule
  * @param version the version of the rule - again tied to ID
  */
case class Id(id: Int, version: Int) extends VersionedId

object LambdaFunction {
  def apply(name: String, rule: String, id: Id): LambdaFunction = LambdaFunctionImpl(name, rule, id)
}

/**
 * The result of serializing or loading rules
 * @param rule
 */
case class ExpressionRule( rule: String ) extends ExprLogic with HasRuleText {
  override def reset(): Unit = super[HasRuleText].reset()
}

/**
 * Used as a result of serializing
 * @param rule
 */
case class OutputExpression( rule: String ) extends OutputExprLogic with HasRuleText with Logging {
  private[quality] override def expression() = {
    val parsed = RuleLogicUtils.expr(rule)
    // output expressions can be:
    // 1. simple expressions for ruleEngine
    // 2. single argument lambda's returning the same type as the arg for folder
    // 3. as of 0.0.2 #8 set( attribute = valueExpression, attribute = valueExpression) converted to the form of 2 with an updateField call
    parsed match {
      case uf: UnresolvedFunction if toName(uf) == "set" =>
        // case 3
        val args = arguments(uf)
        val paired =
          args.flatMap {
            case EqualTo(name: UnresolvedAttribute, right) =>
              // updateField takes paired args of field names to expression
              Some(Seq(Literal(name.name), right))
            case a =>
              logInfo(s"Attempt to convert set OutputExpression argument $a failed as types do not match expected EqualTo(attribute, expression), will default to full expression")
              None
          }

        if (paired.size != args.size)
          // one of the args didn't match type
          parsed
        else
          // need to keep first arg
          UpdateFolderExpression.withArgsAndSubstitutedLambdaVariable(paired.flatten)
      case _ =>
        // for everything else (1+2) it's already good enough
        parsed
    }
  }

  override def reset(): Unit = super[HasRuleText].reset()
}

object RunOnPassProcessor {
  /**
   * Creates a RunOnPassProcesser using a given OutputExpression
   *
   * @param salience
   * @param id
   * @param e
   * @return
   */
  def apply(salience: Int, id: Id, e: OutputExpression) =
    RunOnPassProcessorImpl(salience, id, e.rule, e)

}

/**
  * A rule to run over a row
  * @param id
  * @param expression
  */
case class Rule(id: Id, expression: RuleLogic, runOnPassProcessor: RunOnPassProcessor = NoOpRunOnPassProcessor.noOp) extends Serializable

case class RuleSet(id: Id, rules: Seq[Rule]) extends Serializable

/**
 * Represents a versioned collection of RuleSet's
 * @param id
 * @param ruleSets
 * @param lambdaFunctions
 * @param probablePass override to specify a different percentage for treating probability results as passes - defaults to 80% (0.8)
 */
case class RuleSuite(id: Id, ruleSets: Seq[RuleSet], lambdaFunctions: Seq[LambdaFunction] = Seq.empty, probablePass: Double = 0.8) extends Serializable {

  /**
   * Use a different probable pass value for this RuleSuite
   * @param probablePass
   * @return
   */
  def withProbablePass(probablePass: Double) = copy(probablePass = probablePass)

}
