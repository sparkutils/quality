package com.sparkutils.quality.impl

import com.sparkutils.quality.QualityException
import com.sparkutils.quality.sparkless.impl.DecoderOpEncoderProjection
import com.sparkutils.quality.sparkless.impl.Processors.{NO_QUERY_PLANS, isCopyNeeded}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{Encoder, QualitySparkUtils, ShimUtils}
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.NoOp
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{QualityExprUtils, _}
import org.apache.spark.sql.types.{DataType, StructField, StructType}

//import scala.collection.immutable.{Map, Seq}

/**
 * Delegates to the non var options
 */
object GenerateDecoderOpEncoderVarProjection extends CodeGenerator[Seq[Expression], DecoderOpEncoderProjection[_,_]] {

  // $COVERAGE-OFF$
  protected def canonicalize(in: Seq[Expression]): Seq[Expression] =
    in.map(ExpressionCanonicalizer.execute)

  protected def bind(in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] =
    bindReferences(in, inputSchema)
  // $COVERAGE-ON$

  // $COVERAGE-OFF$
  protected def create(expressions: Seq[Expression]): DecoderOpEncoderProjection[_,_] = ???
  // $COVERAGE-ON$

  def create[I: Encoder, O: Encoder](
                      expressions: Seq[Expression], toSize: Int, allOrdinals: Set[Int]): DecoderOpEncoderProjection[I,O] =
    GenerateDecoderOpEncoderProjection.generate[I, O](expressions, useSubexprElimination = true, toSize)
}
