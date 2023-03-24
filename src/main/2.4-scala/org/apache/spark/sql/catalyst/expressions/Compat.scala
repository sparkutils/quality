package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.types.DataType

trait StatefulLike extends Stateful {
}

trait HigherOrderFunctionLike extends HigherOrderFunction {
  override def bind(f: (Expression, Seq[(DataType, Boolean)]) => LambdaFunction): HigherOrderFunction =
    bindInternal(f)

  protected def bindInternal(f: (Expression, Seq[(DataType, Boolean)]) => LambdaFunction): HigherOrderFunction
}
