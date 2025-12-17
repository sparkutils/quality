package com.sparkutils.quality.impl.aggregates

import com.sparkutils.quality.ClassicOnly
import com.sparkutils.quality.impl.RuleRegistrationFunctions.{defaultAdd, defaultZero}
import org.apache.spark.sql.Column
import org.apache.spark.sql.ShimUtils.{column, expression}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.DataType

trait ClassicAggregateFunctionImports {

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
  @ClassicOnly
  def agg_expr_classic(sumType: DataType, filter: Column, sum: SumExpression, result: ResultsExpression,
               zero: DataType => Option[Any] = defaultZero,
               add: DataType => Option[(Expression, Expression) => Expression] = (dataType: DataType) => defaultAdd(dataType)): Column =
    column( AggregateExpressions(sumType, expression(filter), expression(sum.funN(sumType)), expression(result.funN(sumType)), zero, add, notYetResolved = true) )

}
