package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.trees.TreePattern.{HIGH_ORDER_FUNCTION, TreePattern}
import org.apache.spark.sql.types.{AbstractDataType, DataType}

// NEEDED to work around DBR 9.1 introducing nodePatterns

/**
 * A higher order function takes one or more (lambda) functions and applies these to some objects.
 * The function produces a number of variables which can be consumed by some lambda function.
 */
trait HigherOrderFunction extends Expression with ExpectsInputTypes {

  final val nodePatterns: Seq[TreePattern] = Seq(HIGH_ORDER_FUNCTION)

  override def nullable: Boolean = arguments.exists(_.nullable)

  override def children: Seq[Expression] = arguments ++ functions

  /**
   * Arguments of the higher ordered function.
   */
  def arguments: Seq[Expression]

  def argumentTypes: Seq[AbstractDataType]

  /**
   * All arguments have been resolved. This means that the types and nullabilty of (most of) the
   * lambda function arguments is known, and that we can start binding the lambda functions.
   */
  lazy val argumentsResolved: Boolean = arguments.forall(_.resolved)

  /**
   * Checks the argument data types, returns `TypeCheckResult.success` if it's valid,
   * or returns a `TypeCheckResult` with an error message if invalid.
   * Note: it's not valid to call this method until `argumentsResolved == true`.
   */
  def checkArgumentDataTypes(): TypeCheckResult = {
    ExpectsInputTypes.checkInputDataTypes(arguments, argumentTypes)
  }

  /**
   * Functions applied by the higher order function.
   */
  def functions: Seq[Expression]

  def functionTypes: Seq[AbstractDataType]

  override def inputTypes: Seq[AbstractDataType] = argumentTypes ++ functionTypes

  /**
   * All inputs must be resolved and all functions must be resolved lambda functions.
   */
  override lazy val resolved: Boolean = argumentsResolved && functions.forall {
    case l: LambdaFunction => l.resolved
    case _ => false
  }

  /**
   * Bind the lambda functions to the [[HigherOrderFunction]] using the given bind function. The
   * bind function takes the potential lambda and it's (partial) arguments and converts this into
   * a bound lambda function.
   */
  def bind(f: (Expression, Seq[(DataType, Boolean)]) => LambdaFunction): HigherOrderFunction

  // Make sure the lambda variables refer the same instances as of arguments for case that the
  // variables in instantiated separately during serialization or for some reason.
  @transient lazy val functionsForEval: Seq[Expression] = functions.map {
    case LambdaFunction(function, arguments, hidden) =>
      val argumentMap = arguments.map { arg => arg.exprId -> arg }.toMap
      function.transformUp {
        case variable: NamedLambdaVariable if argumentMap.contains(variable.exprId) =>
          argumentMap(variable.exprId)
      }
  }
}
