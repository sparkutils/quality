package com.sparkutils.quality

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction}
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Expression, Literal, OuterReference, ScalarSubquery, SubqueryExpression, UnresolvedNamedLambdaVariable, LambdaFunction => SparkLambdaFunction}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeFormatter, CodeGenerator, CodegenContext}
import org.apache.spark.sql.qualityFunctions.{FunN, LambdaFunctions, RefExpressionLazyType}
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.{QualitySparkUtils, SparkSession}

import scala.collection.mutable

/**
  * base for storage of rule or ruleset ids, must be a trait to force frameless to use lookup and stop any
  * accidental auto product treatment
  */
sealed trait VersionedId extends Serializable {
  val id, version: Int
}

/**
  * A versioned rule ID - note the name is never persisted in results, the id and version are sufficient to retrieve the name
  * @param id a unique ID to identify this rule
  * @param version the version of the rule - again tied to ID
  */
case class Id(id: Int, version: Int) extends VersionedId

// requires a unique name, otherwise janino can't find the function
object RuleLogicUtils {

  /**
   * Maps a given ruleSuite calling f for each rule allowing transformations
   * @param ruleSuite
   * @param f
   * @return
   */
  def mapRules(ruleSuite: RuleSuite)(f: Rule => Rule) =
    ruleSuite.copy(ruleSets = ruleSuite.ruleSets.map(
      ruleSet =>
        ruleSet.copy( rules = ruleSet.rules.map(
          rule =>
            f(rule)
        ))
    ))

  /**
   * Removes all parsed Expressions.  Subqueries, supported under 3.4 oss / 12.2 dbr v0.0.2, are not serializable until
   * after analysis, as such all expressions must be cleansed
   * @param ruleSuite
   * @return
   */
  def cleanExprs(ruleSuite: RuleSuite) = {
    ruleSuite.lambdaFunctions.foreach(_.reset())
    mapRules(ruleSuite){f => f.expression.reset(); f.runOnPassProcessor.returnIfPassed.reset(); f}
  }

  /**
   * Same as functions.expr without the wrapping Column
   * @param rule
   * @return
   */
  def expr(rule: String) = {
    val parser = SparkSession.getActiveSession.map(_.sessionState.sqlParser).getOrElse {
      QualitySparkUtils.newParser()
    }
    /*
    attempt for a simple expression, then try a plan with exactly one output row.
    "In" will handle ListQuery, "Exists" similarly exists, everything else will likely fail as a parser error
     */
    val rawExpr =
      try {
        parser.parseExpression(rule)
      } catch {
        case ot: Throwable => // e.g. suitable for output expressions
          try {
            ScalarSubquery(parser.parsePlan(rule))
          } catch {
            case _: Throwable =>
              // quite possibly a lambda using a subquery so try and force it via ( sub ) trick,
              // but allow us to replace later after resolution
              val r = rule.split("->")
              if (r.size == 2) {
                val wrapped = s"${r(0)} -> ( ${r(1)} )"
                try {
                  parser.parseExpression(wrapped)
                } catch {
                  case _: Throwable => throw ot // if this didn't work return the original error
                }
              } else
                throw ot // if this didn't work return the original error
          }
      }

    val res =
      rawExpr match {
        case l: SparkLambdaFunction if hasSubQuery(l) =>
          // The lambda's will be parsed as UnresolvedAttributes and not the needed lambdas
          val names = l.arguments.map(a => a.name -> a).toMap
          l.transform {
            case s: SubqueryExpression => s.withNewPlan( s.plan.transform{
              case snippet =>
                snippet.transformAllExpressions {
                  case a: UnresolvedAttribute =>
                    names.get(a.name).map(lamVar => lamVar).getOrElse(a)
                }
            })
          }
        case _ => rawExpr
      }

    res
  }

  def hasSubQuery(expression: Expression): Boolean = expression collect {
    case _: SubqueryExpression => true
  } nonEmpty

  def anyToRuleResult(any: Any): RuleResult =
    any match {
      case b: Boolean => if (b) Passed else Failed
      case 0 | 0.0 => Failed
      case 1 | 1.0 => Passed
      case -1 | -1.0 | "softfail" | "maybe" | "SoftFail" | "softFail" | "Softfail" | "Maybe" => SoftFailed
      case -2  | -2.0 | "disabledrule" | "disabled" | "DisabledRule" | "disabledRule" | "Disabledrule" => DisabledRule
      case d: Double => Probability(d) // only spark 2 unless configured to behave like spark 2
      case d: Decimal => Probability(d.toDouble)
      case "Passed" | "true" | "True" | "passed" | "Pass"| "pass" | "Yes" | "yes" | "1" | "1.0" => Passed
      case "Failed" | "false" | "False" | "failed" | "Fail"| "fail" | "No" | "no" | "0" | "0.0" => Failed
      case _ => Failed // anything else is a fail
    }

  // typically just from compilation
  def anyToRuleResultInt(any: Any): Int =
    any match {
      case b: Boolean => if (b) PassedInt else FailedInt
      case 0 | 0.0 => FailedInt
      case 1 | 1.0 => PassedInt
      case -1 | -1.0 | "softfail" | "maybe" | "SoftFail" | "softFail" | "Softfail" | "Maybe" => SoftFailedInt
      case -2  | -2.0 | "disabledrule" | "disabled" | "DisabledRule" | "disabledRule" | "Disabledrule" => DisabledRuleInt
      case d: Double => (d * PassedInt).toInt
      case d: Decimal => (d.toDouble * PassedInt).toInt
      case "Passed" | "true" | "True" | "passed" | "Pass"| "pass" | "Yes" | "yes" | "1" | "1.0" => PassedInt
      case "Failed" | "false" | "False" | "failed" | "Fail"| "fail" | "No" | "no" | "0" | "0.0" => FailedInt
      case _ => FailedInt // anything else is a fail
    }

}


/**
 * Lambda functions are for re-use across rules. (param: Type, paramN: Type) -> logicResult .
 *
 */
trait LambdaFunction extends HasRuleText with HasExpr {
  val name: String
  val id: Id
  def parsed: LambdaFunctionParsed
}

object LambdaFunction {
  def apply(name: String, rule: String, id: Id): LambdaFunction = LambdaFunctionImpl(name, rule, id)
}

case class LambdaFunctionImpl(name: String, rule: String, id: Id) extends LambdaFunction {
  override def expr: Expression = RuleLogicUtils.expr(rule)

  def parsed: LambdaFunctionParsed = LambdaFunctionParsed(name, rule, id, expr)
}

case class LambdaFunctionParsed(name: String, rule: String, id: Id, override val expr: Expression) extends LambdaFunction {
  def parsed: LambdaFunctionParsed = this
}

trait LambdaFunctions {
  /**
   * This function is for use with directly loading lambda functions to use within normal spark apps.
   *
   * Please be aware loading rules via ruleRunner, addOverallResultsAndDetails or addDataQuality will also load any
   * registered rules applicable for that ruleSuite and may override other registered rules.  You should not use this directly
   * and RuleSuites for that reason (otherwise your results will not be reproducible).
   *
   * @param functions
   */
  def registerLambdaFunctions(functions: Seq[LambdaFunction]): Unit =
    LambdaFunctions.registerLambdaFunctions(functions)

  type IdTriple = (Id, Id, Id)
}

trait RuleLogic extends Serializable {
  def internalEval(internalRow: InternalRow): Any

  def eval(internalRow: InternalRow): RuleResult = {
    val res = internalEval(internalRow)
    RuleLogicUtils.anyToRuleResult(res)
  }

  /**
   * Allows implementations to clear out underlying expressions
   */
  def reset(): Unit = {}
}

trait HasExpr {
  def expr: Expression
}

trait ExprLogic extends RuleLogic with HasExpr {
  override def internalEval(internalRow: org.apache.spark.sql.catalyst.InternalRow) =
    expr.eval(internalRow)
}

trait HasRuleText extends HasExpr {
  val rule: String

  // doesn't need to be serialized, done by RuleRunners
  @volatile
  private[quality] var exprI: Expression = _
  private[quality] def expression(): Expression = {
    if (exprI eq null) {
      exprI = RuleLogicUtils.expr(rule)
    }
    exprI
  }

  def reset(): Unit = {
    exprI = null
  }

  override def expr = expression()
}

/**
 * The result of serializing or loading rules
 * @param rule
 */
case class ExpressionRule( rule: String ) extends ExprLogic with HasRuleText {
  override def reset(): Unit = super[HasRuleText].reset()
}

/**
 * Used in load postprocessing, e.g. coalesce removal, to keep the rule text around
 * @param rule
 * @param expr
 */
case class ExpressionRuleExpr( rule: String, override val expr: Expression ) extends ExprLogic with HasRuleText {
  override def reset(): Unit = super[HasRuleText].reset()
}

trait ExpressionCompiler extends HasExpr {
  lazy val codegen = {
    val ctx = new CodegenContext()
    val eval = expr.genCode(ctx)
    val javaType = CodeGenerator.javaType(expr.dataType)

    val codeBody = s"""
      public scala.Function1<InternalRow, Object> generate(Object[] references) {
        return new ExpressionWrapperEvalImpl(references);
      }

      class ExpressionWrapperEvalImpl extends scala.runtime.AbstractFunction1<InternalRow, Object> {
        private final Object[] references;
        ${ctx.declareMutableStates()}
        ${ctx.declareAddedFunctions()}

        public ExpressionWrapperEvalImpl(Object[] references) {
          this.references = references;
          ${ctx.initMutableStates()}
        }

        public java.lang.Object apply(Object z) {
          InternalRow ${ctx.INPUT_ROW} = (InternalRow) z;
          ${eval.code}
          $javaType temptmep = ${eval.value}; // temp needed for negative values to work, janino gets upset with -  Expression "java.lang.Object" is not an rvalue

          return ${eval.isNull} ? null : ((Object) temptmep);
        }
      }
    """

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))

    val (clazz, _) = CodeGenerator.compile(code)
    val codegen = clazz.generate(ctx.references.toArray).asInstanceOf[InternalRow => AnyRef]

    codegen
  }
}


/**
  * Rewritten children at eval will be swapped out
  * @param expr
  */
case class ExpressionWrapper( expr: Expression, compileEval: Boolean = true) extends ExprLogic with ExpressionCompiler {
  override def internalEval(internalRow: InternalRow): Any = {
    if (compileEval)
      codegen(internalRow)
    else
      super.internalEval(internalRow)
  }
}

trait OutputExprLogic extends HasExpr {
  def eval(internalRow: org.apache.spark.sql.catalyst.InternalRow) =
    expr.eval(internalRow)

  /**
   * Allows clearing of expressions
   */
  def reset(): Unit = {}
}

object UpdateFolderExpression {
  val currentResult = "currentResult"

  import QualitySparkUtils.UnresolvedFunctionOps

  /**
   * Uses the template to add the arguments to the underlying updateField, replacing any currentResult's found with the correct
   * UnresolvedNamedLambdaVariable type.
   * @param args
   * @return
   */
  def withArgsAndSubstitutedLambdaVariable(args: Seq[Expression]): Expression =
    template.copy(function =
      oFunc.withArguments(
        oFunc.theArguments ++ args)).transform{
      case a: UnresolvedAttribute if a.nameParts.head == currentResult =>
        UnresolvedNamedLambdaVariable(a.nameParts)
    }

  lazy val template = RuleLogicUtils.expr(s"$currentResult -> updateField(currentResult)").asInstanceOf[SparkLambdaFunction]
  lazy val oFunc = template.function.asInstanceOf[UnresolvedFunction]

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
      case uf: UnresolvedFunction if VariablesLookup.toName(uf) == "set" =>
        // case 3
        val args = QualitySparkUtils.arguments(uf)
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

/**
 * Used in post serializing processing to keep the rule around
 * @param expr
 */
case class OutputExpressionExpr( rule: String, override val expr: Expression) extends OutputExprLogic with HasRuleText {
  override def reset(): Unit = super[HasRuleText].reset()
}

case class OutputExpressionWrapper( expr: Expression, compileEval: Boolean = true) extends OutputExprLogic with ExpressionCompiler {
  override def eval(internalRow: org.apache.spark.sql.catalyst.InternalRow) =
    if (compileEval)
      codegen(internalRow)
    else
      super.eval(internalRow)
}

trait RunOnPassProcessor extends Serializable {
  def salience: Int
  def id: Id
  def rule: String
  def returnIfPassed: OutputExprLogic
  def withExpr(expr: OutputExpression): RunOnPassProcessor
  def withExpr(expr: OutputExprLogic): RunOnPassProcessor
}

object RunOnPassProcessor {
  def apply(salience: Int, id: Id, e: OutputExpression) =
    RunOnPassProcessorImpl(salience, id, e.rule, e)
}


/**
 * Generates a result upon a pass, it is not evaluated otherwise and only evaluated if no other rule has higher salience.
 * It is not possible to have more than one rule evaluate the returnIfPassed.
 */
case class RunOnPassProcessorImpl(salience: Int, id: Id, rule: String, returnIfPassed: OutputExprLogic) extends RunOnPassProcessor with Serializable {
  override def withExpr(expr: OutputExpression): RunOnPassProcessor =
    copy(rule = expr.rule, returnIfPassed = expr)

  def withExpr(expr: OutputExprLogic): RunOnPassProcessor =
    copy(returnIfPassed = expr)
}

case class HolderUsedInsteadIfImpl(id: Id) extends
  RuntimeException(s"An OutputExpression $id has either not been correctly linked in your rules or you have not called withExpr.")

/**
 * Until output expressions are re-integrated this will throw unimplemented
 * @param salience
 * @param id
 */
case class RunOnPassProcessorHolder(salience: Int, id: Id) extends RunOnPassProcessor with Serializable {
  def returnIfPassed: OutputExprLogic = throw HolderUsedInsteadIfImpl(id)
  def rule: String = throw HolderUsedInsteadIfImpl(id)
  override def withExpr(expr: OutputExpression): RunOnPassProcessor =
    RunOnPassProcessorImpl(salience, id, expr.rule, expr)

  // should not be called
  def withExpr(expr: OutputExprLogic): RunOnPassProcessor = throw HolderUsedInsteadIfImpl(id)
}

object NoOpRunOnPassProcessor {
  val noOpId = Id(Int.MinValue, Int.MinValue)
  val noOp = RunOnPassProcessorImpl(Int.MaxValue, noOpId, "", OutputExpression(""))
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
  def eval(internalRow: InternalRow): RuleSuiteResult = {
    val rawRuleSets =
      ruleSets.map { rs =>
        val ruleSetRawRes = rs.rules.map { r =>
          val ruleResult = r.expression.eval(internalRow)
          r.id -> ruleResult
        }
        val overall = ruleSetRawRes.foldLeft(OverallResult(probablePass)){
          (ov, pair) =>
            ov.process(pair._2)
        }
        rs.id -> RuleSetResult(overall.currentResult, ruleSetRawRes.toMap)
      }
    val overall = rawRuleSets.foldLeft(OverallResult(probablePass)){
      (ov, pair) =>
        ov.process(pair._2.overallResult)
    }

    RuleSuiteResult(id, overall.currentResult, rawRuleSets.toMap)
  }

  /**
   *
   * @param internalRow
   * @return the rulesuiteresult for either archiving (recommended) or discarding and the expression to run to produce the results.
   */
  def evalWithProcessors(internalRow: InternalRow, debugMode: Boolean): (RuleSuiteResult, IdTriple, Any) = {
    // spark 3 definitely better to use sortedmap, stick with cross compilation until it's decided to do the work
    val runOnPassProcessors =
      if (debugMode)
        mutable.ArrayBuffer.empty[(IdTriple, Int, OutputExprLogic)]
      else
        null
    var lowest = (null: IdTriple, Integer.MAX_VALUE, null: OutputExprLogic)

    val rawRuleSets =
      ruleSets.map { rs =>
        val ruleSetRawRes = rs.rules.map { r =>
          val ruleResult = r.expression.eval(internalRow)

          val onPass = ((id, rs.id, r.id), r.runOnPassProcessor.salience, r.runOnPassProcessor.returnIfPassed)
          // only add passed
          if (ruleResult == Passed){
            if (debugMode)
              runOnPassProcessors += (onPass)
            else
              if (r.runOnPassProcessor.salience < lowest._2) {
                lowest = onPass
              }
          }

          r.id -> ruleResult
        }
        val overall = ruleSetRawRes.foldLeft(OverallResult(probablePass)){
          (ov, pair) =>
            ov.process(pair._2)
        }
        rs.id -> RuleSetResult(overall.currentResult, ruleSetRawRes.toMap)
      }
    val overall = rawRuleSets.foldLeft(OverallResult(probablePass)){
      (ov, pair) =>
        ov.process(pair._2.overallResult)
    }

    val (rule, result) =
      if (!debugMode)
        if (lowest._3 ne null)
          (lowest._1, lowest._3.eval(internalRow))
        else
          (null, null)
      else {
        val sorted = runOnPassProcessors.sortBy(_._2)

        if (runOnPassProcessors.isEmpty)
          (null, null)
        else
          (null, new org.apache.spark.sql.catalyst.util.GenericArrayData(sorted.map(p =>
            InternalRow(p._2, p._3.eval(internalRow))
          ).toArray))
      }

    (RuleSuiteResult(id, overall.currentResult, rawRuleSets.toMap), rule, result)
  }

  /**
   *
   * @param inputRow
   * @return the rulesuiteresult for either archiving (recommended) or discarding and the expression to run to produce the results.
   */
  def foldWithProcessors(inputRow: InternalRow, starter: InternalRow, debugMode: Boolean): (RuleSuiteResult, Any) = {
    val runOnPassProcessors =
      mutable.ArrayBuffer.empty[(IdTriple, Int, OutputExprLogic)]

    var row = starter
    /*val results =


    if (debugMode)

    lazy val foldedOnSalience: Seq[(IdTriple, Rule)] =
      ruleSets.flatMap( rs => rs.rules.map(r => r.runOnPassProcessor.salience -> ((id, rs.id, r.id), r))).
        sortBy(_._1).map(_._2)

    */
    val rawRuleSets =
      ruleSets.map { rs =>
        val ruleSetRawRes = rs.rules.map { r =>
          val ruleResult = r.expression.eval(inputRow)

          val onPass = ((id, rs.id, r.id), r.runOnPassProcessor.salience, r.runOnPassProcessor.returnIfPassed)
          // only add passed
          if (ruleResult == Passed){
            runOnPassProcessors += (onPass)
          }

          r.id -> ruleResult
        }
        val overall = ruleSetRawRes.foldLeft(OverallResult(probablePass)){
          (ov, pair) =>
            ov.process(pair._2)
        }
        rs.id -> RuleSetResult(overall.currentResult, ruleSetRawRes.toMap)
      }
    val overall = rawRuleSets.foldLeft(OverallResult(probablePass)){
      (ov, pair) =>
        ov.process(pair._2.overallResult)
    }

    // sort applicable by salience - we don't reset original ordering here - surprising? TODO decide if it is too much surprise
    val sorted = runOnPassProcessors.sortBy(_._2)

    // for each of the output
    // debug copys, non-debug does not
    val res = sorted.foldLeft(Seq.empty[(Int, InternalRow)]){ case (seq, (triple, salience, rule)) =>
      // only accept row - should probably throw something specific when it's not
      // get the lambda's variable to set the current struct
      val FunN(Seq(arg: RefExpressionLazyType), _, _, _, _) = rule.expr

      arg.value = row
      row =  rule.eval(
        inputRow
      ).asInstanceOf[InternalRow]

      seq :+ (salience, if (debugMode)
        row.copy
      else
        row)
    }

    val result =
      if (debugMode)
        new org.apache.spark.sql.catalyst.util.GenericArrayData(res.map(p =>
          InternalRow(p._1, p._2)
        ).toArray)
      else {
        if (res.isEmpty)
          null
        else
          res.last._2
      }

    (RuleSuiteResult(id, overall.currentResult, rawRuleSets.toMap), result)
  }

  /**
   * Use a different probable pass value for this RuleSuite
   * @param probablePass
   * @return
   */
  def withProbablePass(probablePass: Double) = copy(probablePass = probablePass)
}
