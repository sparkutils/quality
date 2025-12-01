package com.sparkutils.quality

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

case class LambdaFunction(name: String, id: Id, rule: String) extends HasRuleText {
}

/**
  * A versioned rule ID - note the name is never persisted in results, the id and version are sufficient to retrieve the name
  * @param id a unique ID to identify this rule
  * @param version the version of the rule - again tied to ID
  */
@SerialVersionUID(1L)
case class Id(id: Int, version: Int) extends VersionedId

/**
 * The result of serializing or loading rules
 * @param rule
 */
@SerialVersionUID(1L)
case class ExpressionRule( rule: String ) extends HasRuleText

/**
 * Used as a result of serializing
 * @param rule
 */
@SerialVersionUID(1L)
case class OutputExpression( rule: String ) extends HasRuleText

@SerialVersionUID(1L)
case class RunOnPassProcessor(salience: Int, id: Id, rule: String) extends HasRuleText with Serializable

object NoOpRunOnPassProcessor {
  val noOpId = Id(Serializing.notPresentOutputId, Serializing.notPresentOutputVersion)
  val noOp = RunOnPassProcessor(Serializing.notPresentSalience, noOpId, "")
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
