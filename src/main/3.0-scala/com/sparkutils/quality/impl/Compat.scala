package com.sparkutils.quality.impl

import org.apache.spark.sql.catalyst.expressions.{Expression, HigherOrderFunction, Stateful, LambdaFunction => SLambdaFunction}
import org.apache.spark.sql.types.DataType

trait StatefulLike extends Stateful {
}

trait HigherOrderFunctionLike extends HigherOrderFunction {
  override def bind(f: (Expression, Seq[(DataType, Boolean)]) => SLambdaFunction): HigherOrderFunction =
    bindInternal(f)

  protected def bindInternal(f: (Expression, Seq[(DataType, Boolean)]) => SLambdaFunction): HigherOrderFunction
}