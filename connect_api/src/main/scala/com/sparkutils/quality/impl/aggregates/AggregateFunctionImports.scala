package com.sparkutils.quality.impl.aggregates

import com.sparkutils.quality.QualityException
import org.apache.spark.sql.{Column, ShimUtils}
import org.apache.spark.sql.ShimUtils.callFunction
import org.apache.spark.sql.functions.{lit, struct}
import org.apache.spark.sql.shim.utils.createLambda
import org.apache.spark.sql.types.{DataType, LongType, MapType}

sealed trait SumExpression {
  protected[quality] def funN(sumType: DataType): Column

}
protected[quality] case class SumWith(lambdaFunctionIn: Column, name: String = "sum_with") extends SumExpression {
  override def funN(sumType: DataType): Column =
    ShimUtils.callFunction("sum_with", lit(sumType.sql), lambdaFunctionIn)
}
protected[quality] case class SumWithMap(id: Column, lambdaFunctionIn: Column) extends SumExpression {
  override def funN(sumType: DataType): Column = sumType match {
    case mt: MapType =>
      ShimUtils.callFunction("map_with", lit(sumType.sql), id.cast(mt.keyType), lambdaFunctionIn)
    case _ =>
      throw QualityException("You must use a MapType dataType when using map_with")
  }
}

sealed trait ResultsExpression {
  def funN(sumType: DataType): Column
}
protected[quality] case class ResultsWith(lambdaFunctionIn: Column, name: String = "results_with") extends ResultsExpression {
  override def funN(sumType: DataType): Column =
    // NB - this implementation could just forward to results_with but this tests out the use of FunN and RefExpression
    ShimUtils.callFunction("qualityfunn", callFunction("qualityrefexpression", lit(sumType.sql)),
      callFunction("qualityrefexpression", lit(LongType.sql)), lambdaFunctionIn,
      lit(name), lit(false), lit(true))
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
   * @return
   */
  def agg_expr(sumType: DataType, filter: Column, sum: SumExpression, result: ResultsExpression): Column =
    ShimUtils.callFunction("agg_expr", lit(sumType.sql), filter, sum.funN(sumType), result.funN(sumType) )

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
  def map_with(id: Column, sum: Column => Column): SumExpression =
    SumWithMap(id, createLambda(sum))

  /**
   * Aggregates over RuleSuiteResults columns and returns a RuleSuiteGroupStatistics row
   * @param results
   */
  def rule_suite_statistics(results: Column): Column =
    ShimUtils.callFunction("rule_suite_statistics", results)

  /**
   * Prefer rule_suite_statistics, this function is only provided as a fallback should there be issues in the rule_suite_statistics implementation, which is over 6% faster.
   *
   * Aggregates over RuleSuiteResults columns and returns a RuleSuiteGroupStatistics row using a pre Spark 4 unified API
   * Aggregator, per https://github.com/sparkutils/quality/issues/117 this does not run on Databricks Shared Clusters.   *
   * @param results
   */
  @deprecated(message = "This aggregation implementation will be removed in 0.3.0 and should only be used if rule_suite_statistics has issues", since = "0.2.0")
  def rule_suite_statistics_aggregator(results: Column): Column =
    ShimUtils.callFunction("rule_suite_statistics_aggregator", results)
}
