package com.sparkutils.quality.impl

import com.sparkutils.quality
import com.sparkutils.quality.VariablesLookup.Identifiers
import com.sparkutils.quality.utils.RuleSuiteDocs.{IdTrEither, LambdaId, OutputExpressionId, RuleId}
import com.sparkutils.quality.utils.{Docs, DocsParser, RuleSuiteDocs, WithDocs}
import com.sparkutils.quality.{ExpressionLookup, ExpressionRule, HasExpr, HasRuleText, Id, NoOpRunOnPassProcessor, OutputExpression, Rule, RuleLogicUtils, RuleSuite, RunOnPassProcessor, VariablesLookup, namesFromSchema}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.{Column, DataFrame, QualitySparkUtils, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Expression, LambdaFunction, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

sealed trait RuleRelevant
sealed trait LambdaRelevant
sealed trait OutputExpressionRelevant

sealed trait HasId {
  def id: Id
}

sealed trait HasOutputText {
  def outputText: String
}

sealed trait HasNonIdText {
  def nonIdText: String
}

/**
 * Base for RuleWarnings
 */
sealed trait RuleWarning extends HasId with HasOutputText with HasNonIdText {
  def warning: String

  override def nonIdText: String = warning

  /**
   * If the error is syntax based - defined by parsing, rather than any later stage
   * @return
   */
  def syntax: Boolean = false

  def warningText = s"$warning, occurred when processing id $id"

  override def outputText: String = warningText
}

sealed trait SyntaxWarning extends RuleWarning {
  final override def syntax: Boolean = true
}

sealed trait SyntaxNameWarning extends SyntaxWarning {
  def name: String
}

case class LambdaPossibleSOE(id: Id) extends RuleWarning with LambdaRelevant {
  val warning = "Possible SOE detected"
}

case class NonLambdaDocParameters(id: Id) extends SyntaxWarning {
  val warning = "Parameter documentation is present on a non lambda expression"
}

case class ExtraDocParameter(id: Id, name: String) extends SyntaxNameWarning with LambdaRelevant {
  val warning = s"Parameter $name is not found in the lambda expression"
}

/**
 * Base for RuleErrors
 */
sealed trait RuleError extends HasId with HasOutputText with HasNonIdText {
  def error: String

  override def nonIdText: String = error
  /**
   * If the error is syntax based - defined by parsing, rather than any later stage
   * @return
   */
  def syntax: Boolean = false

  def errorText = s"$error occurred when processing id $id"

  override def outputText: String = errorText
}

sealed trait SyntaxError extends RuleError {
  final override def syntax: Boolean = true
}

sealed trait NameMissingError extends RuleError {
  def name: String
  final override def error = s"Name $name is missing"
}

sealed trait ViewMissingError extends RuleError {
  def name: String
  final override def error = s"View $name is missing"
}

case class LambdaSyntaxError(id: Id, error: String) extends SyntaxError with LambdaRelevant
case class LambdaStackOverflowError(id: Id) extends SyntaxError with LambdaRelevant {
  val error = "A lambda function seems to infinitely recurse"
}
case class LambdaNameError(name: String, id: Id) extends NameMissingError with LambdaRelevant
case class LambdaMultipleImplementationWithSameArityError(name: String, count: Int, argLength: Int, ids: Set[Id]) extends SyntaxError with LambdaRelevant {
  val error = s"Lambda function $name has $count implementations with $argLength arguments"
  val id = ids.head
}
case class LambdaViewError(name: String, id: Id) extends ViewMissingError with LambdaRelevant

case class RuleSyntaxError(id: Id, error: String) extends SyntaxError with RuleRelevant
case class RuleNameError(name: String, id: Id) extends NameMissingError with RuleRelevant
case class RuleViewError(name: String, id: Id) extends ViewMissingError with RuleRelevant

case class OutputRuleSyntaxError(id: Id, error: String) extends SyntaxError with OutputExpressionRelevant
case class OutputRuleNameError(name: String, id: Id) extends NameMissingError with OutputExpressionRelevant
case class OutputRuleViewError(name: String, id: Id) extends ViewMissingError with OutputExpressionRelevant

case class LambdaSparkFunctionNameError(name: String, id: Id) extends NameMissingError with LambdaRelevant
case class SparkFunctionNameError(name: String, id: Id) extends NameMissingError with RuleRelevant
case class OuputSparkFunctionNameError(name: String, id: Id) extends NameMissingError with OutputExpressionRelevant

case class DataFrameSyntaxError(error: String) extends SyntaxError {
  val id = Validation.dataFrameSyntaxErrorId
}

object Validation {
  val unknownSOEId = Id(Int.MinValue,Int.MinValue)
  val dataFrameSyntaxErrorId = Id(Int.MinValue+1,Int.MinValue+1)
}

/**
 * Paramters to pass into showString for debugging / validation
 * @param numRows defaults to 1000
 * @param truncate
 * @param vertical
 */
case class ShowParams(numRows: Int = 1000, truncate: Int = 0, vertical: Boolean = false)

trait Validation {

  protected val defaultViewLookup: String => Boolean =
    SparkSession.active.catalog.tableExists(_)

  /**
   * For a given dataFrame provide a full set of any validation errors for a given ruleSuite.
   *
   * @param schema which fields should the dataframe have
   * @param ruleSuite
   * @return A set of errors and the output from the dataframe when a runnerFunction is specified
   */
  def validate_Lookup(schema: StructType, ruleSuite: RuleSuite, viewLookup: String => Boolean): (Set[RuleError], Set[RuleWarning]) = {
    val (err, warns, out, docs, exp) = validate(Left(schema), ruleSuite, viewLookup = viewLookup)
    (err, warns)
  }

  /**
   * For a given dataFrame provide a full set of any validation errors for a given ruleSuite.
   *
   * @param schema which fields should the dataframe have
   * @param ruleSuite
   * @return A set of errors and the output from the dataframe when a runnerFunction is specified
   */
  def validate(schema: StructType, ruleSuite: RuleSuite): (Set[RuleError], Set[RuleWarning]) = {
    val (err, warns, out, docs, exp) = validate(Left(schema), ruleSuite)
    (err, warns)
  }

  /**
   * For a given dataFrame provide a full set of any validation errors for a given ruleSuite.
   *
   * @param frame when it's a Left( StructType ) the struct will be used to test against and an emptyDataframe of this type created to resolve on the spark level.  Using Right(DataFrame) will cause that dataframe to be used which is great for test cases with a runnerFunction
   * @param ruleSuite
   * @return A set of errors and the output from the dataframe when a runnerFunction is specified
   */
  def validate_Lookup(frame: DataFrame, ruleSuite: RuleSuite, viewLookup: String => Boolean = defaultViewLookup): (Set[RuleError], Set[RuleWarning]) = {
    val (err, warns, out, docs, exp) = validate(Right(frame), ruleSuite, viewLookup = viewLookup)
    (err, warns)
  }

  /**
   * For a given dataFrame provide a full set of any validation errors for a given ruleSuite.
   *
   * @param frame when it's a Left( StructType ) the struct will be used to test against and an emptyDataframe of this type created to resolve on the spark level.  Using Right(DataFrame) will cause that dataframe to be used which is great for test cases with a runnerFunction
   * @param ruleSuite
   * @return A set of errors and the output from the dataframe when a runnerFunction is specified
   */
  def validate(frame: DataFrame, ruleSuite: RuleSuite): (Set[RuleError], Set[RuleWarning]) = {
    val (err, warns, out, docs, exp) = validate(Right(frame), ruleSuite)
    (err, warns)
  }

  /**
   * For a given dataFrame provide a full set of any validation errors for a given ruleSuite.
   *
   * @param frame when it's a Left( StructType ) the struct will be used to test against and an emptyDataframe of this type created to resolve on the spark level.  Using Right(DataFrame) will cause that dataframe to be used which is great for test cases with a runnerFunction
   * @param ruleSuite
   * @param runnerFunction - allows you to create a ruleRunner or ruleEngineRunner with different configurations
   * @return A set of errors and the output from the dataframe when a runnerFunction is specified
   *
   */
  def validate_Lookup(frame: DataFrame, ruleSuite: RuleSuite, runnerFunction: DataFrame => Column, viewLookup: String => Boolean): (Set[RuleError], Set[RuleWarning], String, RuleSuiteDocs, Map[IdTrEither, ExpressionLookup]) =
    validate(Right(frame), ruleSuite, runnerFunction = Some(runnerFunction), viewLookup = viewLookup)


  /**
   * For a given dataFrame provide a full set of any validation errors for a given ruleSuite.
   *
   * @param frame when it's a Left( StructType ) the struct will be used to test against and an emptyDataframe of this type created to resolve on the spark level.  Using Right(DataFrame) will cause that dataframe to be used which is great for test cases with a runnerFunction
   * @param ruleSuite
   * @param runnerFunction - allows you to create a ruleRunner or ruleEngineRunner with different configurations
   * @return A set of errors and the output from the dataframe when a runnerFunction is specified
   */
  def validate(frame: DataFrame, ruleSuite: RuleSuite, runnerFunction: DataFrame => Column): (Set[RuleError], Set[RuleWarning], String, RuleSuiteDocs, Map[IdTrEither, ExpressionLookup]) =
    validate(Right(frame), ruleSuite, runnerFunction = Some(runnerFunction))

  /**
   * For a given dataFrame provide a full set of any validation errors for a given ruleSuite.
   *
   * @param frame when it's a Left( StructType ) the struct will be used to test against and an emptyDataframe of this type created to resolve on the spark level.  Using Right(DataFrame) will cause that dataframe to be used which is great for test cases with a runnerFunction
   * @param ruleSuite
   * @param runnerFunction - allows you to create a ruleRunner or ruleEngineRunner with different configurations
   * @param transformBeforeShow - an optional transformation function to help shape what results are pushed to show
   * @return A set of errors and the output from the dataframe when a runnerFunction is specified
   */
  def validate(frame: DataFrame, ruleSuite: RuleSuite, runnerFunction: DataFrame => Column, transformBeforeShow: DataFrame => DataFrame, viewLookup: String => Boolean): (Set[RuleError], Set[RuleWarning], String, RuleSuiteDocs, Map[IdTrEither, ExpressionLookup]) =
    validate(Right(frame), ruleSuite, runnerFunction = Some(runnerFunction), transformBeforeShow = transformBeforeShow, viewLookup = viewLookup)

  /**
   * For a given dataFrame provide a full set of any validation errors for a given ruleSuite.
   *
   * @param frame when it's a Left( StructType ) the struct will be used to test against and an emptyDataframe of this type created to resolve on the spark level.  Using Right(DataFrame) will cause that dataframe to be used which is great for test cases with a runnerFunction
   * @param ruleSuite
   * @param runnerFunction - allows you to create a ruleRunner or ruleEngineRunner with different configurations
   * @param transformBeforeShow - an optional transformation function to help shape what results are pushed to show
   * @return A set of errors and the output from the dataframe when a runnerFunction is specified
   */
  def validate(frame: DataFrame, ruleSuite: RuleSuite, runnerFunction: DataFrame => Column, transformBeforeShow: DataFrame => DataFrame): (Set[RuleError], Set[RuleWarning], String, RuleSuiteDocs, Map[IdTrEither, ExpressionLookup]) =
    validate(Right(frame), ruleSuite, runnerFunction = Some(runnerFunction), transformBeforeShow = transformBeforeShow)

  val emptyDocs = Docs()

  /**
   * For a given dataFrame provide a full set of any validation errors for a given ruleSuite.
   *
   * @param schemaOrFrame when it's a Left( StructType ) the struct will be used to test against and an emptyDataframe of this type created to resolve on the spark level.  Using Right(DataFrame) will cause that dataframe to be used which is great for test cases with a runnerFunction
   * @param ruleSuite
   * @param runnerFunction - allows you to create a ruleRunner or ruleEngineRunner with different configurations
   * @param showParams - configure how the output text is formatted using the same options and formatting as dataFrame.show
   * @param qualityName - the column name to store the runnerFunction results in
   * @param recursiveLambdasSOEIsOk - this signals that finding a recursive lambda SOE should not stop the evaluations - if true it will still try to run any runnerFunction but may not give the correct results
   * @param transformBeforeShow - an optional transformation function to help shape what results are pushed to show
   * @param viewLookup - for any subquery used looks up the view name for being present (quoted and with schema), defaults to the current spark catalogue
   * @return A set of errors and the output from the dataframe when a runnerFunction is specified
   */
  def validate(schemaOrFrame: Either[StructType, DataFrame], ruleSuite: RuleSuite, showParams: ShowParams = ShowParams(),
               runnerFunction: Option[DataFrame => Column] = None, qualityName: String = "Quality",
               recursiveLambdasSOEIsOk: Boolean = false, transformBeforeShow: DataFrame => DataFrame = identity, viewLookup: String => Boolean = defaultViewLookup):
                (Set[RuleError], Set[RuleWarning], String, RuleSuiteDocs, Map[IdTrEither, ExpressionLookup]) = {
    val schema = schemaOrFrame.fold(identity, _.schema)

    val names = namesFromSchema(schema)

    val docsWarnings = mutable.Set[RuleWarning]()

    val ((lambdaSyntaxErrors, lambdaLookups, potentialOverflows, unknownLambdaSparkFunctionErrors, lambdaArityErrors, lambdaNameErrors, lambdas, lambdaDocWarnings, lambadaExpressionLookups, lambdaViewErrors)) =
      validateLambdas(ruleSuite, recursiveLambdasSOEIsOk, names, viewLookup) match {
        case Left(toReturn) => return toReturn
        case Right(result) => result
      }

    docsWarnings ++= lambdaDocWarnings

    val (ruleErrors, ruleDocWarnings, rules, outputExpressions, ruleExpressionLookups) = validateRules(ruleSuite, lambdaLookups, names, viewLookup)

    docsWarnings ++= ruleDocWarnings

    val (showOut, dfErrors) =
      validateAgainstDataFrame(schemaOrFrame, showParams, runnerFunction, qualityName, transformBeforeShow, schema, viewLookup)

    (unknownLambdaSparkFunctionErrors ++ lambdaArityErrors ++ dfErrors ++ ruleErrors ++ lambdaNameErrors ++ lambdaSyntaxErrors.map(_._2.right.get).toSet ++ lambdaViewErrors,
      potentialOverflows.map( LambdaPossibleSOE ) ++ (Set() ++ docsWarnings)
      , showOut, RuleSuiteDocs(rules, outputExpressions, lambdas), lambadaExpressionLookups ++ ruleExpressionLookups)
  }

  protected def validateAgainstDataFrame(schemaOrFrame: Either[StructType, DataFrame], showParams: ShowParams, runnerFunction: Option[DataFrame => Column], qualityName: String, transformBeforeShow: DataFrame => DataFrame, schema: StructType, viewLookup: String => Boolean) = {
    val basedf = schemaOrFrame.right.getOrElse {
      val session = SparkSession.active
      val empty = session.sparkContext.emptyRDD[Row]
      session.createDataFrame(empty, schema)
    }

    val (showOut, dfErrors) =
      runnerFunction.fold(("", Set.empty[RuleError]))(rf => {
        val runner = rf(basedf)
        try {
          val withRules = basedf.withColumn(qualityName, runner)
          val transformed = transformBeforeShow(withRules)
          (QualitySparkUtils.toString(transformed, showParams), Set.empty)
        } catch {
          case e: Throwable => ("", Set(DataFrameSyntaxError(e.getMessage)))
        }
      })
    (showOut, dfErrors)
  }

  protected def validateRules(ruleSuite: RuleSuite, lambdaLookups: Map[String, Map[Id, Set[String]]], names: Set[String], viewLookup: String => Boolean)= {
    val doRule = validateRule(lambdaLookups, names) _

    var rules = Map.empty[Id, WithDocs[Rule]]
    var outputExpressions = Map.empty[Id, WithDocs[RunOnPassProcessor]]
    var exprLookups = Map.empty[IdTrEither, ExpressionLookup]

    val docsWarnings = mutable.Set[RuleWarning]()

    def addDocs[T](id: Id, rule: T, expressionRule: HasRuleText): (Id, WithDocs[T]) =
      DocsParser.parse(expressionRule.rule).map { parseddocs =>
        val res = id -> WithDocs(rule, parseddocs)
        if (parseddocs.params.nonEmpty) {
          docsWarnings += NonLambdaDocParameters(id)
        }
        res
      }.getOrElse(id -> WithDocs(rule, emptyDocs))

    // do the rules
    val ruleErrors =
      ruleSuite.ruleSets.flatMap { rs =>
        rs.rules.flatMap { r =>
          rules += addDocs(r.id, r, r.expression.asInstanceOf[HasRuleText])

          val (ruleErrors, exprLookup) = doRule(r.id, r.expression.asInstanceOf[HasExpr].expr, false, viewLookup)
          exprLookups += RuleId(r.id) -> exprLookup

          val outputErrors =
            if (r.runOnPassProcessor != NoOpRunOnPassProcessor.noOp) {
              outputExpressions += addDocs(r.runOnPassProcessor.id, r.runOnPassProcessor, r.runOnPassProcessor.returnIfPassed.asInstanceOf[OutputExpression])

              val (oErrors, oExprLookup) = doRule(r.runOnPassProcessor.id, r.runOnPassProcessor.returnIfPassed.expr, true, viewLookup)
              exprLookups += OutputExpressionId(r.runOnPassProcessor.id) -> oExprLookup
              oErrors
            } else
              Set.empty

          ruleErrors ++ outputErrors
        }
      }.toSet

    (ruleErrors, Set() ++ docsWarnings, Map() ++ rules, outputExpressions, Map() ++ exprLookups)
  }

  protected def validateLambdas(ruleSuite: RuleSuite, recursiveLambdasSOEIsOk: Boolean, names: Set[String], viewLookup: String => Boolean): Either[(Set[RuleError], Set[RuleWarning], String, RuleSuiteDocs, Map[IdTrEither, ExpressionLookup]),
    (Seq[(String, Either[(Id, Expression), LambdaSyntaxError])], Map[String, Map[Id, Set[String]]],
      Set[Id], Set[LambdaSparkFunctionNameError], Set[LambdaMultipleImplementationWithSameArityError], Set[LambdaNameError], Map[Id, WithDocs[quality.LambdaFunction]], Set[RuleWarning], Map[IdTrEither, ExpressionLookup], Set[LambdaViewError])] = {

    var lambdas = Map.empty[Id, WithDocs[quality.LambdaFunction]]
    val docsWarnings = mutable.Set[RuleWarning]()

    val viewErrors = ruleSuite.lambdaFunctions.flatMap { f =>
      subQueryErrors(viewLookup, f.expr, LambdaViewError(_, f.id))
    }.toSet

    val (lambdaLeftExpressions, lambdaSyntaxErrors) = ruleSuite.lambdaFunctions.map { f =>
      (f.name,
        try {
          val expr = f.expr
          val ret = Left((f.id, expr))

          val args =
            expr match {
              case lambda: LambdaFunction => lambda.arguments.map(VariablesLookup.toName).toSet
              case _ => Set.empty[String]
            }

          DocsParser.parse(f.rule).map { parseddocs =>
            lambdas += f.id -> WithDocs(f, parseddocs)

            parseddocs.params.keySet.foreach { name =>
              if (!args.contains(name)) {
                docsWarnings += ExtraDocParameter(f.id, name)
              }
            }
          }.getOrElse {
            lambdas += f.id -> WithDocs(f, emptyDocs)
          }

          ret
        } catch {
          case e: Throwable => Right(LambdaSyntaxError(f.id, e.getMessage))
        })
    }.partition {
      _._2.isLeft
    }

    val lambdaNameToExpressions = lambdaLeftExpressions.groupBy(p => p._1).mapValues(e => e.map(_._2.left.get).toMap)

    val (lambdaLookups, potentialOverflows, unknownLambdaSparkFunctions) = try {
      VariablesLookup.processLambdas(lambdaNameToExpressions)
    } catch {
      // SOE is possible with lambdas calling lambdas, capture that as a distinct issue
      case soe: StackOverflowError =>
        if (recursiveLambdasSOEIsOk)
        // type needed otherwise it gets stuck with the first param type derivation _1 <: String instead of String
          (Map.empty[String, Map[Id, Identifiers]], Set.empty[Id], Map.empty[Id, Set[String]])
        else
          return Left((Set(LambdaStackOverflowError(Validation.unknownSOEId)), Set.empty[RuleWarning], "", RuleSuiteDocs(), Map.empty[IdTrEither, ExpressionLookup]))
    }

    // now that they are looked up, a bit duplicative but...
    val exprLookups = lambdaNameToExpressions.values.flatMap( m => m.map(pair => LambdaId(pair._1) -> VariablesLookup.fieldsFromExpression(pair._2, lambdaLookups))).toMap

    val unknownLambdaSparkFunctionErrors = unknownLambdaSparkFunctions.flatMap(p => p._2.map(name =>
      LambdaSparkFunctionNameError(name, p._1))).toSet

    val lambdaArityErrors = lambdaNameToExpressions.filter(p => p._2.size > 1).flatMap {
      pairs =>
        val map = pairs._2

        val counts = map.groupBy(_._2.children.size - 1) // one child is the return
        val moreThan1 = counts.collectFirst { case f if f._2.size > 1 => f }
        moreThan1.map { f =>
          LambdaMultipleImplementationWithSameArityError(pairs._1, f._2.size, f._1, f._2.keySet)
        }
    }.toSet

    // do we have variables used in the lambdas which are not in the schema?
    val lambdaNameErrors: Set[LambdaNameError] =
      lambdaLookups.flatMap { p =>
        p._2.flatMap { pair =>
          val (id, identifiers) = pair
          if (identifiers.diff(names).isEmpty)
            None
          else
            Some(identifiers.diff(names).map(LambdaNameError(_, id)))
        }
      }.flatten.toSet
    Right((lambdaSyntaxErrors, lambdaLookups, potentialOverflows, unknownLambdaSparkFunctionErrors, lambdaArityErrors, lambdaNameErrors, Map() ++ lambdas, Set() ++ docsWarnings, exprLookups, viewErrors))
  }

  protected def subQueryErrors[T](lookup: String => Boolean, expression: Expression, f: String => T): Set[T] = (expression collect {
    case s: SubqueryExpression => s.plan.collect{
      case rel: UnresolvedRelation if !lookup(rel.name) =>
        f(rel.name)
    }
  }).flatten.toSet

  protected def validateRule(lambdaLookups: Map[String, Map[Id, Set[String]]], names: Set[String])(id: Id, exprThunk: => Expression, outputRule: Boolean, viewLookup: String => Boolean): (Set[RuleError], ExpressionLookup) =
    try {
      val expr = exprThunk
      val exl @ ExpressionLookup(exprFields, unknownSparkFunctions, _, _) = VariablesLookup.fieldsFromExpression(expr, lambdaLookups)
      val rules = exprFields.flatMap{
        field =>
          if (names.contains(field))
            None
          else
            Some(
              if (!outputRule)
                RuleNameError(field, id)
              else
                OutputRuleNameError(field, id)
            )
      }.toSet[RuleError]

      val viewErrors = subQueryErrors(viewLookup, exprThunk, if (outputRule) OutputRuleViewError(_, id) else RuleViewError(_, id))

      val unknown = unknownSparkFunctions.map{ name =>
        if (!outputRule)
          SparkFunctionNameError(name, id)
        else
          OuputSparkFunctionNameError(name, id)
      }

      (rules ++ unknown ++ viewErrors, exl)
    } catch {
      case e: Throwable => (Set(
        if (!outputRule)
          RuleSyntaxError(id, e.getMessage)
        else
          OutputRuleSyntaxError(id, e.getMessage)
      ), ExpressionLookup())
    }

}
