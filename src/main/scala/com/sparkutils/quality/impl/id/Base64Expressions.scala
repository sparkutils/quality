package com.sparkutils.quality.impl.id

import java.util.Base64

import com.sparkutils.quality.impl.id.model.GuaranteedUniqueIDType
import org.apache.spark.sql.InputTypeChecks
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.Cast.toSQLValue
import org.apache.spark.sql.catalyst.expressions.ExpectsInputTypes.{toSQLExpr, toSQLType}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, UnaryExpression}
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * for a given string returns the length of the given id in longs
 * @param child
 */
case class SizeOfIDString(child: Expression) extends UnaryExpression with InputTypeChecks with CodegenFallback {
  protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)

  override def nullSafeEval(input: Any): Any = {
    val bytes = Base64.getDecoder.decode(input.toString)
    model.idTypeOf(bytes(0)) match {
      case GuaranteedUniqueIDType => 2
      case _ =>
        model.lengthOfID(bytes)
    }
  }

  override def dataType: DataType = IntegerType

  override def inputDataTypes: Seq[Seq[DataType]] = Seq(Seq(StringType))
}

/**
 * For an id structure generates a string
 * @param child
 */
case class AsBase64Struct(child: Expression) extends UnaryExpression with InputTypeChecks with CodegenFallback {
  protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)

  /**
   * used with SizeOfIDString to verify strings are comparable
   */
  lazy val size = child.dataType.asInstanceOf[StructType].fields.size - 1

  override def nullSafeEval(input: Any): Any = {
    val struct = input.asInstanceOf[InternalRow]
    val base = struct.getInt(0)
    val longs = (1 to size).map(i => struct.getLong(i)).toArray
    UTF8String.fromString( model.base64( model.bitLength(size), base, longs) )
  }

  override def dataType: DataType = StringType

  override def checkInputDataTypes(): TypeCheckResult = child.dataType match {
    case StructType(fields) if fields.head.name.endsWith("_base") && fields.head.dataType == IntegerType &&
      fields.drop(1).zipWithIndex.forall{ case (f, i) => f.name.endsWith(s"_i$i") && f.dataType == LongType } =>
      TypeCheckResult.TypeCheckSuccess
    case _ => DataTypeMismatch(
      errorSubClass = "UNEXPECTED_INPUT_TYPE",
      messageParameters = Map(
        "paramIndex" -> "1",
        "requiredType" -> "<.._base: INT, .._i0: BIGINT, .._i1: BIGINT>",
        "inputSql" -> toSQLExpr(child),
        "inputType" -> toSQLType(child.dataType)
      )
    )
  }

  override def inputDataTypes: Seq[Seq[DataType]] = Seq()
}

/**
 * For an id structure generates a string, first arg is an int, the others long
 * @param children
 */
case class AsBase64Fields(children: Seq[Expression]) extends Expression with InputTypeChecks with CodegenFallback {

  /**
   * used with SizeOfIDString to verify strings are comparable
   */
  lazy val size = children.size - 1

  override def eval(input: InternalRow = null): Any = {
    val base = children(0).eval(input)
    val longs = (1 to size).map(i => children(i).eval(input)).toArray
    if (base == null || longs.exists(_ == null))
      null
    else
      UTF8String.fromString( model.base64(model.bitLength(size), base.asInstanceOf[Int], longs.map(_.asInstanceOf[Long])) )
  }

  override def dataType: DataType = StringType

  override def checkInputDataTypes(): TypeCheckResult = children.map(_.dataType) match {
    case s: Seq[DataType] if s.size < 3 =>
      DataTypeMismatch(
        errorSubClass = "VALUE_OUT_OF_RANGE",
        messageParameters = Map(
          "exprName" -> "arguments",
          "valueRange" -> s"[3, positive]",
          "currentValue" -> toSQLValue(s.size, IntegerType)
        )
      )
    case IntegerType +: tail =>
      tail.zipWithIndex.map{
        case (t, _) if t == LongType => TypeCheckResult.TypeCheckSuccess
        case (t, i) => DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> (i + 2).toString, // 1 is first param
            "requiredType" -> toSQLType(LongType),
            "inputSql" -> toSQLExpr(children(i + 1)),
            "inputType" -> toSQLType(t)
          )
        )
      }.find{ case _: DataTypeMismatch => true case _ => false }.getOrElse(TypeCheckResult.TypeCheckSuccess)

    case _ => DataTypeMismatch(
      errorSubClass = "UNEXPECTED_INPUT_TYPE",
      messageParameters = Map(
        "paramIndex" -> "1",
        "requiredType" -> toSQLType(IntegerType),
        "inputSql" -> toSQLExpr(children(0)),
        "inputType" -> toSQLType(children(0).dataType)
      )
    )
  }

  override def nullable: Boolean = children.exists(_.nullable)

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(children = newChildren)

  override def inputDataTypes: Seq[Seq[DataType]] = Seq()
}

/**
 * Generates an unprefixed 'raw' id structure of a given size.  Note that size is fixed, the type can't change on the plan during the plan.
 *
 * @param child the base64 strings that must have the same size, will return null if it's not the right size, or cannot parse it.
 * @param size the size for the number of longs to have, 2 longs is 160 bit and the default
 */
case class IDFromBase64(child: Expression, size: Int) extends UnaryExpression with InputTypeChecks with CodegenFallback {

  override def nullSafeEval(child: Any): Any =  try {
    val id = model.parseID(child.toString).asInstanceOf[BaseWithLongs]
    val ar = id.array
    if (ar.length != size)
      null
    else
      InternalRow.fromSeq(id.base +: ar.toSeq)
  } catch {
    case _: Throwable => null
  }

  override def nullable: Boolean = true

  override def dataType: DataType = model.rawType(size)

  override def inputDataTypes: Seq[Seq[DataType]] = Seq(Seq(StringType))

  protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)
}