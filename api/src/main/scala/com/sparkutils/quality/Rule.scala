package com.sparkutils.quality

import com.sparkutils.quality.RunOnPassProcessor.RunOnPassProcessorImpl
import com.sparkutils.quality.impl.util.Serializing

/**
 * base for storage of rule or ruleset ids, must be a trait to force frameless to use lookup and stop any
 * accidental auto product treatment
 */
trait VersionedId extends Serializable {
  val id, version: Int
}

trait HasRuleText extends Serializable {
  val rule: String
}

/**
 * A user defined SQL function
 */
trait LambdaFunction extends HasRuleText {
  val name: String
  val id: Id
}

object LambdaFunction {
  @SerialVersionUID(1L)
  protected[quality] case class LambdaFunctionImpl(name: String, rule: String, id: Id) extends LambdaFunction

  def apply(name: String, rule: String, id: Id): LambdaFunction =
    LambdaFunctionImpl(name, rule, id)
}

/**
  * A versioned rule ID - note the name is never persisted in results, the id and version are sufficient to retrieve the name
  * @param id a unique ID to identify this rule
  * @param version the version of the rule - again tied to ID
  */
@SerialVersionUID(1L)
case class Id(id: Int, version: Int) extends VersionedId

/**
 * A trigger rule
 */
trait ExpressionRule extends HasRuleText

object ExpressionRule {
  @SerialVersionUID(1L)
  case class ExpressionRuleImpl( rule: String ) extends ExpressionRule

  def apply(rule: String): ExpressionRule = ExpressionRuleImpl(rule)
}

/**
 * Used as a result of serializing
 */
trait OutputExpression extends HasRuleText

object OutputExpression {
  @SerialVersionUID(1L)
  case class OutputExpressionImpl( rule: String ) extends OutputExpression

  def apply(rule: String): OutputExpression = OutputExpressionImpl(rule)
}

/**
 * Configuration of what should be evaluated when a trigger passes and salience matches
 */
trait RunOnPassProcessor extends HasRuleText {
  val salience: Int
  val id: Id
  def withExpr(e: OutputExpression): RunOnPassProcessor
}

object RunOnPassProcessor {
  @SerialVersionUID(1L)
  case class RunOnPassProcessorImpl(salience: Int, id: Id, rule: String) extends RunOnPassProcessor with Serializable {
    override def withExpr(e: OutputExpression): RunOnPassProcessor = copy(rule = e.rule)
  }

  /**
   * Creates a RunOnPassProcesser using a given OutputExpression
   *
   * @param salience
   * @param id
   * @param e
   * @return
   */
  def apply(salience: Int, id: Id, e: OutputExpression) =
    RunOnPassProcessorImpl(salience, id, e.rule)
}

object NoOpRunOnPassProcessor {
  val noOpId = Id(Serializing.notPresentOutputId, Serializing.notPresentOutputVersion)
  val noOp = RunOnPassProcessorImpl(Serializing.notPresentSalience, noOpId, "")
}

/**
  * A rule to run over a row
  * @param id
  * @param expression
  */
@SerialVersionUID(1L)
case class Rule(id: Id, expression: ExpressionRule, runOnPassProcessor: RunOnPassProcessor = NoOpRunOnPassProcessor.noOp) extends Serializable

@SerialVersionUID(1L)
case class RuleSet(id: Id, rules: Seq[Rule]) extends Serializable

/**
 * Represents a versioned collection of RuleSet's
 * @param id
 * @param ruleSets
 * @param lambdaFunctions
 * @param probablePass override to specify a different percentage for treating probability results as passes - defaults to 80% (0.8)
 */
@SerialVersionUID(1L)
case class RuleSuite(id: Id, ruleSets: Seq[RuleSet], lambdaFunctions: Seq[LambdaFunction] = Seq.empty, probablePass: Double = 0.8) extends Serializable {

  /**
   * Use a different probable pass value for this RuleSuite
   * @param probablePass
   * @return
   */
  def withProbablePass(probablePass: Double) = copy(probablePass = probablePass)

}
