package com.sparkutils.quality.impl.id

import com.sparkutils.quality.QualityException.qualityException
import com.sparkutils.quality.impl.hash.{HashFunctionFactory, HashFunctionsExpression, MessageDigestFactory}
import com.sparkutils.quality.impl.rng.{RandLongsWithJump, RandomLongs}
import org.apache.commons.rng.simple.RandomSource
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.qualityFunctions.DigestFactory
import org.apache.spark.sql.types.{ArrayType, DataType, LongType, StructType}



/**
 * Delegates ID creation to some other expression which must provide an array of longs result.
 *
 * @param id type of the GenericLong compatible ID to be generated
 * @param child an expression generating an Array Of Longs
 * @param prefix how to name the field
 */
case class GenericLongBasedIDExpression(id: IDType, child: Expression, prefix: String) extends UnaryExpression with CodegenFallback {
  private lazy val (converter, length) = GenericLongBasedIDExpression.longArrayConverter(child)
  private lazy val headerHolder = GenericLongBasedID(id, Array.ofDim[Long](length))
  private lazy val base = headerHolder.base

  override protected def nullSafeEval(input: Any): Any = {
    // an array of longs
    val ar = converter(input)
    // fill the data back out
    InternalRow.fromSeq(base +: ar)
  }

//  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = ???

  override def dataType: DataType = headerHolder.dataType(prefix)

  protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)
}

object GenericLongBasedIDExpression {

  def longArrayConverter( expr: Expression): (Any => Array[Long], Int) = {
    val struct =
      expr.dataType match{
        case e: StructType => e
        case _ => null
      }

    if (expr.dataType == RandomLongs.structType || (
      (struct ne null) &&
      struct.fields.forall(_.dataType == LongType)
    )) {
      val count = struct.fields.length
      ((input: Any) => {
        val row = input.asInstanceOf[InternalRow]
        (0 until count).map(row.getLong).toArray[Long]
      }, count)
    } else
      qualityException("Only structures with arrays of longs are supported")
  }

}