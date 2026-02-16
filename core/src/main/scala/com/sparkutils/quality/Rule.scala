package com.sparkutils.quality

import com.sparkutils.quality.DefaultProcessor.DefaultProcessorImpl
import com.sparkutils.quality.NoOpRunOnPassProcessor.noOpId
import com.sparkutils.quality.RuleSuite.defaultProbablePass
import com.sparkutils.quality.RunOnPassProcessor.RunOnPassProcessorImpl

/**
 * base for storage of rule or ruleset ids, must be a trait to force frameless to use lookup and stop any
 * accidental auto product treatment
 */
sealed trait VersionedId extends Serializable {
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
trait ExpressionRule extends Serializable {
  def updateRule(rule: String): ExpressionRule
}

object ExpressionRule {
  @SerialVersionUID(1L)
  case class ExpressionRuleImpl( rule: String ) extends ExpressionRule with HasRuleText {
    override def updateRule(rule: String): ExpressionRule = copy(rule = rule)
  }

  def apply(rule: String): ExpressionRule = ExpressionRuleImpl(rule)
}

/**
 * Used as a result of serializing
 */
trait OutputExpression

object OutputExpression {
  @SerialVersionUID(1L)
  case class OutputExpressionImpl( rule: String ) extends OutputExpression with HasRuleText

  def apply(rule: String): OutputExpression = OutputExpressionImpl(rule)
}

trait HasOutputExpression extends Serializable {
  type ThisType
  val id: Id
  def outputExpression: OutputExpression
  def withExpr(e: OutputExpression): ThisType
}

/**
 * Configuration of what should be evaluated when a trigger passes and salience matches
 */
trait RunOnPassProcessor extends HasRuleText with HasOutputExpression {
  val salience: Int
  val id: Id
  val returnIfPassed: OutputExpression
  type ThisType = RunOnPassProcessor

  override def outputExpression: OutputExpression = returnIfPassed
  def withExpr(e: OutputExpression): RunOnPassProcessor
}

object RunOnPassProcessor {
  @SerialVersionUID(1L)
  case class RunOnPassProcessorImpl(salience: Int, id: Id, rule: String, returnIfPassed: OutputExpression) extends RunOnPassProcessor with Serializable {
    override def withExpr(e: OutputExpression): RunOnPassProcessor = copy(returnIfPassed = e)
  }

  /**
   * Creates a RunOnPassProcesser using a given OutputExpression
   *
   * @param salience
   * @param id
   * @param e
   * @return
   */
  def apply(salience: Int, id: Id, e: OutputExpression): RunOnPassProcessor =
    RunOnPassProcessorImpl(salience, id, e match {
      case h: HasRuleText => h.rule
      case _ => ""
    }, e)
}

object NoOpRunOnPassProcessor {

  val notPresentSalience: Int = 1234567890
  val notPresentOutputId: Int = Int.MinValue
  val notPresentOutputVersion: Int = Int.MinValue

  val noOpId = Id(notPresentOutputId, notPresentOutputVersion)
  val noOp = RunOnPassProcessorImpl(notPresentSalience, noOpId, "", OutputExpression(""))
}

/**
 * Configuration of what should be evaluated when no trigger passes for Folder and Collector
 */
trait DefaultProcessor extends HasRuleText with HasOutputExpression {
  type ThisType = DefaultProcessor
  val id: Id
  val outputExpression: OutputExpression
  def withExpr(e: OutputExpression): DefaultProcessor
}

object DefaultProcessor {
  @SerialVersionUID(1L)
  case class DefaultProcessorImpl(id: Id, rule: String, outputExpression: OutputExpression) extends DefaultProcessor with Serializable {
    override def withExpr(e: OutputExpression): DefaultProcessor = copy(outputExpression = e)
  }

  /**
   * Creates a DefaultProcessor using a given OutputExpression
   *
   * @param id
   * @param e
   * @return
   */
  def apply(id: Id, e: OutputExpression): DefaultProcessor =
    DefaultProcessorImpl(id, e match {
      case h: HasRuleText => h.rule
      case _ => ""
    }, e)
}

object NoOpDefaultProcessor {
  val noOp = DefaultProcessorImpl(noOpId, "", OutputExpression(""))
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
 * @param defaultProcessor when using folder or collector this output expression will be used when no rules are triggered
 */
@SerialVersionUID(1L)
case class RuleSuite(id: Id, ruleSets: Seq[RuleSet], lambdaFunctions: Seq[LambdaFunction] = Seq.empty,
                     probablePass: Double = defaultProbablePass, defaultProcessor: DefaultProcessor = NoOpDefaultProcessor.noOp) extends Serializable {

  /**
   * Use a different probable pass value for this RuleSuite
   * @param probablePass
   * @return
   */
  def withProbablePass(probablePass: Double) = copy(probablePass = probablePass)

  /**
   * Sorts the ruleSets and lambdaFunctions by Id
   * @return
   */
  def sorted: RuleSuite =
    copy(
      ruleSets = ruleSets.sortBy(l => (l.id.id, l.id.version)).map(r =>
        r.copy(rules = r.rules.sortBy(l => (l.id.id, l.id.version)).toVector)).toVector,
      lambdaFunctions = lambdaFunctions.sortBy(l => (l.id.id, l.id.version)).toVector)
}

object RuleSuite {
  val defaultProbablePass: Double = 0.8d

  /**
   * Maps a given ruleSuite calling f for each rule allowing transformations
   *
   * @param ruleSuite
   * @param f
   * @return
   */
  def mapRules(ruleSuite: RuleSuite)(f: Rule => Rule) =
    ruleSuite.copy(ruleSets = ruleSuite.ruleSets.map(
      ruleSet =>
        ruleSet.copy(rules = ruleSet.rules.map(
          rule =>
            f(rule)
        ))
    ))
}