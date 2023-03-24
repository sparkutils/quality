package org.apache.spark.sql.catalyst.expressions

trait StatefulLike extends Nondeterministic {
  /**
   * Return a fresh uninitialized copy of the stateful expression.
   */
  def freshCopy(): StatefulLike

}

trait HigherOrderFunctionLike extends HigherOrderFunction {}

