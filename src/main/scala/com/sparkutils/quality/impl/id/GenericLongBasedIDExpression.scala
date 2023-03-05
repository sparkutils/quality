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

trait GenericLongBasedImports {
  /**
   * Creates a default randomRNG based on RandomSource.XO_RO_SHI_RO_128_PP
   */
  def rngID(prefix: String): Column =
    new Column(GenericLongBasedIDExpression(model.RandomID,
      RandLongsWithJump(0L, RandomSource.XO_RO_SHI_RO_128_PP), prefix))

  /**
   * Creates a hash based ID based on a 128 bit MD5 by default
   * @param prefix
   * @return
   */
  def fieldBasedID(prefix: String, children: Seq[Column], digestImpl: String = "MD5", digestFactory: String => DigestFactory = MessageDigestFactory): Column =
    new Column(GenericLongBasedIDExpression(model.FieldBasedID,
      HashFunctionsExpression(children.map(_.expr), digestImpl, true, digestFactory(digestImpl)), prefix))

  def fieldBasedID(prefix: String, digestImpl: String, children: Column *): Column =
    fieldBasedID(prefix, children, digestImpl)

  /**
   * Creates a hash based ID based on an upstream compatible long generator
   * @param prefix
   * @return
   */
  def providedID(prefix: String, child: Column): Column =
    new Column(GenericLongBasedIDExpression(model.ProvidedID, child.expr, prefix))

  /**
   * Murmur3 hash
   * @param prefix
   * @param children
   * @param digestImpl - only Murmur3 currently supported
   * @return
   */
  def hashID(prefix: String, children: Seq[Column], digestImpl: String = "IGNORED"): Column =
    new Column(GenericLongBasedIDExpression(model.FieldBasedID,
      HashFunctionsExpression(children.map(_.expr), digestImpl, true, HashFunctionFactory("IGNORED")), prefix))

  def hashID(prefix: String, digestImpl: String, children: Column*): Column = hashID(prefix, children, digestImpl)

  /**
   * Murmur3 hash
   * @param prefix
   * @param children
   * @return
   */
  def murmur3ID(prefix: String, children: Seq[Column]): Column = hashID(prefix, children, "M3_128")
  def murmur3ID(prefix: String, child1: Column, restOfchildren: Column*): Column = hashID(prefix, child1 +: restOfchildren, "M3_128")

}

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