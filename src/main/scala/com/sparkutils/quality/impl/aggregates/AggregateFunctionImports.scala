package com.sparkutils.quality.impl.aggregates

import com.sparkutils.quality.QualityException
import com.sparkutils.quality.impl.RuleRegistrationFunctions.{defaultAdd, defaultZero}
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{Expression, LambdaFunction}
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.qualityFunctions.{FunN, MapTransform, RefExpression}
import org.apache.spark.sql.shim.utils.createLambda
import org.apache.spark.sql.types.{DataType, LongType, MapType}

sealed trait SumExpression {
  protected[quality] def funN(sumType: DataType): Expression
}
protected[quality] case class SumWith(lambdaFunctionIn: LambdaFunction, name: String = "sum_with") extends SumExpression {
  override def funN(sumType: DataType): Expression = FunN(Seq(RefExpression(sumType)), lambdaFunctionIn, Some(name))
}
protected[quality] case class SumWithMap(id: Column, lambdaFunctionIn: LambdaFunction, zero: DataType => Option[Any]) extends SumExpression {
  override def funN(sumType: DataType): Expression = sumType match {
    case mt: MapType =>
      MapTransform.create(RefExpression(sumType), id.cast(mt.keyType).expr, lambdaFunctionIn, zero)
    case _ =>
      throw QualityException("You must use a MapType dataType when using map_with")
  }
}

sealed trait ResultsExpression {
  def funN(sumType: DataType): Expression
}
protected[quality] case class ResultsWith(lambdaFunctionIn: LambdaFunction, name: String = "results_with") extends ResultsExpression {
  override def funN(sumType: DataType): Expression = FunN(Seq(RefExpression(sumType), RefExpression(LongType)), lambdaFunctionIn, Some(name))
}

trait AggregateFunctionImports {

  /**
   * Creates an aggregate by applying filter to rows, calling sum with a starting value (provided by zero) and finally calls result to process the sum and count values for a final result.
   *
   * Note, when working with lambda's in the dsl it's often required to use the dataframes col function as the scope is incorrect in the lambda.
   *
   * @param sumType the type used to sum across rows
   * @param filter filter only input rows interesting to count (similar to CountIf, SumIf)
   * @param sum add to the current sum, takes the current sum as the parameter, sum_with, inc, map_with etc. can be used as implementations
   * @param result processes the sum result and row count (after filtering) to produce the final result of the aggregate
   * @param zero the default value for a given sumType
   * @param add the default addition logic for a given sumType, which combines the sumType across partitions
   * @return
   */
  def agg_expr(sumType: DataType, filter: Column, sum: SumExpression, result: ResultsExpression,
               zero: DataType => Option[Any] = defaultZero _,
               add: DataType => Option[(Expression, Expression) => Expression] = (dataType: DataType) => defaultAdd(dataType)): Column =
    new Column( AggregateExpressions(sumType, filter.expr, sum.funN(sumType), result.funN(sumType), zero, add, notYetResolved = true) )

  /**
   * Given the current sum, produce the next sum, for example by incrementing 1 on the sum to count filtered rows
   * @param sum
   * @return
   */
  def sum_with(sum: Column => Column): SumExpression =
    SumWith(createLambda(sum))

  /**
   * Produces an aggregate result
   * @param result the sum and count are parameters
   * @return
   */
  def results_with(result: (Column, Column) => Column): ResultsExpression =
    ResultsWith(createLambda(result))

/*
  val incX = (exps: Seq[Expression]) => exps match {
    case Seq(x: AttributeReference) =>
      val name = x.qualifier.mkString(".") + x.name // that is bad code man should be option
      sumWith(s"sum -> sum + $name")(Seq())
    case Seq(Literal(str: UTF8String, StringType)) =>
      // case for type passing
      sumWith("sum -> sum + 1")(exps)
    case Seq(Literal(str: UTF8String, StringType), x: AttributeReference) =>
      val name = x.qualifier.mkString(".") + x.name
      sumWith(s"sum -> sum + $name")(Seq(exps(0))) // keep the type, drop the attr
    case Seq(Literal(str: UTF8String, StringType), y) =>
      qualityException(INC_REWRITE_GENEXP_ERR_MSG)
    case Seq( y ) =>
      val SLambdaFunction(a: Add, Seq(sum: UnresolvedNamedLambdaVariable), hidden ) = functions.expr("sumWith(sum -> sum + 1)").expr.children(0)
      import QualitySparkUtils.{add => addf}
      // could be a cast around x or three attributes plusing each other or....
      FunN(Seq(RefExpression(LongType)),
        SLambdaFunction(addf(a.left, y, LongType), Seq(sum), hidden )
        , Some("inc")) // keep the type
  */
  /**
   * Adds 1L to the sum value
   * @return
   */
  val inc: SumExpression =
    SumWith(createLambda(sum => sum + 1L), "inc")

  /**
   * Adds incrementWith to the sum value
   * @param incrementWith
   * @return
   */
  def inc(incrementWith: Column): SumExpression =
    SumWith(createLambda(sum => sum + incrementWith), "inc")

  /**
   * Provides the mean (summed value / count of filtered rows)
   * @return
   */
  val meanf: ResultsExpression =
    ResultsWith(createLambda((sum, count) => sum / count), "meanf")

  /**
   * returns the sum, ignoring the count
   */
  val return_sum: ResultsExpression =
    ResultsWith(createLambda((sum, count) => sum), "return_sum")

  /**
   * returns both the count and sum
   */
  val return_both: ResultsExpression =
    ResultsWith(createLambda((sum, count) => struct(sum, count)), "return_both")

  /**
   * Creates an entry in a map sum with id and the result of 'sum' with the previous sum at that id as it's parameter.
   * @param id
   * @param sum the parameter is the previous value of maps' id entry
   * @param zero the default value for the map's value type
   * @return
   */
  def map_with(id: Column, sum: Column => Column, zero: DataType => Option[Any] = defaultZero _): SumExpression =
    SumWithMap(id, createLambda(sum), zero)
}
