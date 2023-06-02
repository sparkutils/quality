package com.sparkutils.quality.impl

import org.apache.spark.sql.catalyst.expressions.{Expression, HigherOrderFunction, Nondeterministic, LambdaFunction => SLambdaFunction}
import org.apache.spark.sql.types.DataType

// SPARK-41049 is backported
trait StatefulLike extends Nondeterministic {
  /**
   * Return a fresh uninitialized copy of the stateful expression.
   */
  def freshCopy(): StatefulLike

}

// this is not backported and is 3.3.2 based (still)
trait HigherOrderFunctionLike extends HigherOrderFunction {
  override def bind(f: (Expression, Seq[(DataType, Boolean)]) => SLambdaFunction): HigherOrderFunction =
    bindInternal(f)

  protected def bindInternal(f: (Expression, Seq[(DataType, Boolean)]) => SLambdaFunction): HigherOrderFunction
}