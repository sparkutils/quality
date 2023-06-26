package com.sparkutils.quality.impl

import com.sparkutils.quality.impl.longPair.LongPair

import java.util.UUID
import com.sparkutils.quality.impl.rng.RandomLongs
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.qualityFunctions.InputTypeChecks
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

object RngUUIDExpression {
  def binary(input: Any): UTF8String = {
    val bytes = input.asInstanceOf[Array[Byte]]
    require(bytes.length == 16, "Only byte arrays with length of two longs can be used (16)")
    getNextUUIDString(bytes)
  }
  def struct(input: Any): UTF8String = {
    val iRow = input.asInstanceOf[InternalRow]
    getNextUUIDString( iRow.getLong(1), iRow.getLong(0) )
  }

  def getLongs(b: Array[Byte]): (Long, Long) = {
    val l = (b(7).asInstanceOf[Long] << 56) | (b(6).asInstanceOf[Long] & 0xff) << 48 | (b(5).asInstanceOf[Long] & 0xff) << 40 |
      (b(4).asInstanceOf[Long] & 0xff) << 32 | (b(3).asInstanceOf[Long] & 0xff) << 24 | (b(2).asInstanceOf[Long] & 0xff) << 16 |
      (b(1).asInstanceOf[Long] & 0xff) << 8 | (b(0).asInstanceOf[Long] & 0xff)
    val h = (b(15).asInstanceOf[Long] << 56) | (b(14).asInstanceOf[Long] & 0xff) << 48 | (b(13).asInstanceOf[Long] & 0xff) << 40 |
      (b(12).asInstanceOf[Long] & 0xff) << 32 | (b(11).asInstanceOf[Long] & 0xff) << 24 | (b(10).asInstanceOf[Long] & 0xff) << 16 |
      (b(9).asInstanceOf[Long] & 0xff) << 8 | (b(8).asInstanceOf[Long] & 0xff)

    (l, h)
  }

  def getNextUUID(most: Long, least: Long): UUID = {
    val mostSigBits = (most & 0xFFFFFFFFFFFF0FFFL) | 0x0000000000004000L
    val leastSigBits = (least | 0x8000000000000000L) & 0xBFFFFFFFFFFFFFFFL

    new UUID(mostSigBits, leastSigBits)
  }

  def getNextUUIDString(bytes: Array[Byte]) = UTF8String.fromString((getNextUUID _).tupled(getLongs(bytes)).toString)

  def getNextUUIDString(most: Long, least: Long) = UTF8String.fromString(getNextUUID(most, least).toString)

}

/**
 * Creates a uuid from byte arrays or two longs
 * @param child the expression to produce a BinaryType
 */
case class RngUUIDExpression(child: Expression) extends UnaryExpression with InputTypeChecks {
  override def dataType: DataType = StringType

  lazy val evalF = child.dataType match {
    case BinaryType => RngUUIDExpression.binary _
    case RandomLongs.structType => RngUUIDExpression.struct _
  }

  override def nullSafeEval(input: Any): Any = {
    evalF(input)
  }

  override def inputDataTypes: Seq[Seq[DataType]] = Seq(Seq(RandomLongs.structType, BinaryType))

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, c =>
      s"com.sparkutils.quality.impl.RngUUIDExpression.${
        child.dataType match {
          case BinaryType => "binary"
          case RandomLongs.structType => "struct"
        }
      }($c)")

  protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)
}

object UUIDToLongsExpression {
  def toLongs(input: Any) = {
    val uuid = UUID.fromString(input.toString)
    InternalRow(uuid.getLeastSignificantBits, uuid.getMostSignificantBits)
  }
}

/**
 * Pulls Longs out from a uuid
 * @param child the UUID expression
 */
case class UUIDToLongsExpression(child: Expression) extends UnaryExpression with InputTypeChecks {
  //require(child.dataType == StringType, "Only String types are possible parameters")
  override def dataType: DataType = LongPair.structType

  override def nullSafeEval(input: Any): Any = UUIDToLongsExpression.toLongs(input)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, c => s"com.sparkutils.quality.impl.UUIDToLongsExpression.toLongs($c)")

  override def inputDataTypes: Seq[Seq[DataType]] = Seq(Seq(StringType))

  protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)
}
