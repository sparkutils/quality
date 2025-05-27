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
import org.apache.spark.sql.catalyst.expressions.codegen.{ExprUtils, _}
import org.apache.spark.sql.types.{DataType, StructField, StructType}

//import scala.collection.immutable.{Map, Seq}

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

  def create[I: Encoder, O: Encoder](
                      expressions: Seq[Expression], toSize: Int,
                      allOrdinals: Set[Int]): DecoderOpEncoderProjection[I,O] =
    throw new Exception("Not supported on any Databricks runtime")
}
