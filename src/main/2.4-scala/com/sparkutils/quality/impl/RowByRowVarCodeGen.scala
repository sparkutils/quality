package com.sparkutils.quality.impl

import com.sparkutils.quality.sparkless.impl.DecoderOpEncoderProjection
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.expressions._

/**
 * CODE is based on MutableProjection, but uses currentVars and wholestage elimination and generates a transformation between
 * two encoders over a middle operation.
 * The generated class itself can create new instances directly, unlike Spark projections that need to go through the
 * codegen source generation cycle, only the compilation is cached.
 *
 * If the expression tree contains stateful expressions with codegenfallback the code must be regenerated against a
 * fresh tree.
 */
object GenerateDecoderOpEncoderVarProjection {

  // $COVERAGE-OFF$
  def create[I: Encoder, O: Encoder](
                      expressions: Seq[Expression], toSize: Int,
                      allOrdinals: Set[Int]): DecoderOpEncoderProjection[I,O] =
    throw new Exception("Not supported on the 2.4 runtime")
  // $COVERAGE-ON$
}
