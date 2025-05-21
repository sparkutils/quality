package com.sparkutils.quality.impl

import com.sparkutils.quality.QualityException
import com.sparkutils.quality.sparkless.impl.DecoderOpEncoderProjection
import com.sparkutils.quality.sparkless.impl.Processors.{NO_QUERY_PLANS, isCopyNeeded}
import org.apache.spark.sql.{Encoder, ShimUtils}
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.NoOp
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.types.StructType

/**
 * CODE is based on MutableProjection and generates a transformation between two encoders over a middle operation.
 * The generated class itself can create new instances directly, unlike Spark projections that need to go through the
 * codegen source generation cycle, only the compilation is cached.
 *
 * If the expression tree contains stateful expressions with codegenfallback the code must be regenerated against a
 * fresh tree.
 */
object GenerateDecoderOpEncoderProjection {
  def generate[I: Encoder, O: Encoder](expressions: Seq[Expression],
                                       useSubexprElimination: Boolean): DecoderOpEncoderProjection[I,O] =
    throw new Exception("Not supported on any 2.4 runtime")

}
