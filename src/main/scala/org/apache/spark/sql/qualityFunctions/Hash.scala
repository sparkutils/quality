package org.apache.spark.sql.qualityFunctions

import org.apache.spark.sql.QualitySparkUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

import scala.annotation.tailrec

//CTw - this is copied 1:1 from the main dist replacing E and Long with Array[Long] for variable length hashes
// seed gets replaced with a type that returns Array[Long] and includes generation / reset for each new digest with
// a clear MessageDigest impl

/**
 * Basic digest implementation for Array[Long] based hashes
 */
trait Digest {
  def hashInt(i: Int): Unit

  def hashLong(l: Long): Unit

  def hashBytes(base: Array[Byte], offset: Int, length: Int): Unit

  def digest: Array[Long]
}

/**
 * Factory to get a new or reset digest for each row
 */
trait DigestFactory extends Serializable {
  def fresh: Digest
  def length: Int
}

/**
 * A function that calculates hash value for a group of expressions.  Note that the `seed` argument
 * is not exposed to users and should only be set inside spark SQL.
 *
 * The hash value for an expression depends on its type and seed:
 *  - null:                    seed
 *  - boolean:                 turn boolean into int, 1 for true, 0 for false,
 *                             and then use murmur3 to hash this int with seed.
 *  - byte, short, int:        use murmur3 to hash the input as int with seed.
 *  - long:                    use murmur3 to hash the long input with seed.
 *  - float:                   turn it into int: java.lang.Float.floatToIntBits(input), and hash it.
 *  - double:                  turn it into long: java.lang.Double.doubleToLongBits(input),
 *                             and hash it.
 *  - decimal:                 if it's a small decimal, i.e. precision <= 18, turn it into long
 *                             and hash it. Else, turn it into bytes and hash it.
 *  - calendar interval:       hash `microseconds` first, and use the result as seed
 *                             to hash `months`.
 *  - interval day to second:  it store long value of `microseconds`, use murmur3 to hash the long
 *                             input with seed.
 *  - interval year to month:  it store int value of `months`, use murmur3 to hash the int
 *                             input with seed.
 *  - binary:                  use murmur3 to hash the bytes with seed.
 *  - string:                  get the bytes of string and hash it.
 *  - array:                   The `result` starts with seed, then use `result` as seed, recursively
 *                             calculate hash value for each element, and assign the element hash
 *                             value to `result`.
 *  - struct:                  The `result` starts with seed, then use `result` as seed, recursively
 *                             calculate hash value for each field, and assign the field hash value
 *                             to `result`.
 *
 * Finally we aggregate the hash values for each expression by the same way of struct.
 */
abstract class HashLongsExpression extends Expression with CodegenFallback {
  val factory: DigestFactory

  val asStruct: Boolean

  override def dataType: DataType =
    if (asStruct)
      StructType(
        (0 until factory.length).map(i => StructField(name = "i"+i, dataType = LongType))
      )
    else
      ArrayType(LongType)

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = false

  private def hasMapType(dt: DataType): Boolean = {
    dt.existsRecursively(_.isInstanceOf[MapType])
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length < 1) {
      TypeCheckResult.TypeCheckFailure(
        s"input to function $prettyName requires at least one argument")
    } /*
original code but we'll assume it can't be disabled
    else if (children.exists(child => hasMapType(child.dataType)) &&
      !SQLConf.get.getConf(SQLConf.LEGACY_ALLOW_HASH_ON_MAPTYPE)) {

      TypeCheckResult.TypeCheckFailure(
        s"input to function $prettyName cannot contain elements of MapType. In Spark, same maps " +
          "may have different hashcode, thus hash expressions are prohibited on MapType elements." +
          s" To restore previous behavior set ${SQLConf.LEGACY_ALLOW_HASH_ON_MAPTYPE.key} " +
          "to true.")
       */
    else if (children.exists(child => hasMapType(child.dataType))) {
      TypeCheckResult.TypeCheckFailure(
        s"input to function $prettyName cannot contain elements of MapType. In Spark, same maps " +
          "may have different hashcode, thus hash expressions are prohibited on MapType elements.")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def eval(input: InternalRow = null): Any = {
    val hash = factory.fresh
    var i = 0
    val len = children.length
    while (i < len) {
      computeHash(children(i).eval(input), children(i).dataType, hash)
      i += 1
    }
    if (asStruct)
      InternalRow(hash.digest :_*) // make the array nested
    else
      new GenericArrayData(hash.digest)
  }

  protected def computeHash(value: Any, dataType: DataType, hash: Digest): Unit
/*
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    ev.isNull = FalseLiteral

    val childrenHash = children.map { child =>
      val childGen = child.genCode(ctx)
      childGen.code + ctx.nullSafeExec(child.nullable, childGen.isNull) {
        computeHash(childGen.value, child.dataType, ev.value, ctx)
      }
    }

    val hashResultType = "Long[]"
    val typedSeed = s"org.apache.spark.sql.qualityFunctions.Digest"
    val codes = ctx.splitExpressionsWithCurrentInputs(
      expressions = childrenHash,
      funcName = "computeHash",
      extraArguments = Seq(), // hashResultType -> ev.value , not needed
      returnType = "void",
      makeSplitFunction = body =>
        s"""
           |$body
         """.stripMargin,
      foldFunctions = _.map(funcCall => s"${ev.value} = $funcCall;").mkString("\n"))

    ev.copy(code =
      code"""
            |$hashResultType ${ev.value} = $typedSeed;
            |$codes
       """.stripMargin)
  }
*/
  protected def nullSafeElementHash(
                                     input: String,
                                     index: String,
                                     nullable: Boolean,
                                     elementType: DataType,
                                     result: String,
                                     ctx: CodegenContext): String = {
    val element = ctx.freshName("element")

    val jt = CodeGenerator.javaType(elementType)
    ctx.nullSafeExec(nullable, s"$input.isNullAt($index)") {
      s"""
        final $jt $element = ${CodeGenerator.getValue(input, elementType, index)};
        ${computeHash(element, elementType, result, ctx)}
      """
    }
  }

  protected def genHashInt(i: String, result: String): String =
    s"$result = $hasherClassName.hashInt($i, $result);"

  protected def genHashLong(l: String, result: String): String =
    s"$result = $hasherClassName.hashLong($l, $result);"

  protected def genHashBytes(b: String, result: String): String = {
    val offset = "Platform.BYTE_ARRAY_OFFSET"
    s"$result = $hasherClassName.hashUnsafeBytes($b, $offset, $b.length, $result);"
  }

  protected def genHashBoolean(input: String, result: String): String =
    genHashInt(s"$input ? 1 : 0", result)

  protected def genHashFloat(input: String, result: String): String = {
    s"""
       |if($input == -0.0f) {
       |  ${genHashInt("0", result)}
       |} else {
       |  ${genHashInt(s"Float.floatToIntBits($input)", result)}
       |}
     """.stripMargin
  }

  protected def genHashDouble(input: String, result: String): String = {
    s"""
       |if($input == -0.0d) {
       |  ${genHashLong("0L", result)}
       |} else {
       |  ${genHashLong(s"Double.doubleToLongBits($input)", result)}
       |}
     """.stripMargin
  }

  protected def genHashDecimal(
                                ctx: CodegenContext,
                                d: DecimalType,
                                input: String,
                                result: String): String = {
    if (d.precision <= Decimal.MAX_LONG_DIGITS) {
      genHashLong(s"$input.toUnscaledLong()", result)
    } else {
      val bytes = ctx.freshName("bytes")
      s"""
         |final byte[] $bytes = $input.toJavaBigDecimal().unscaledValue().toByteArray();
         |${genHashBytes(bytes, result)}
       """.stripMargin
    }
  }

  protected def genHashTimestamp(t: String, result: String): String = genHashLong(t, result)

  protected def genHashCalendarInterval(input: String, result: String): String = {
    val microsecondsHash = s"$hasherClassName.hashLong($input.microseconds, $result)"
    s"$result = $hasherClassName.hashInt($input.months, $microsecondsHash);"
  }

  protected def genHashString(input: String, result: String): String = {
    val baseObject = s"$input.getBaseObject()"
    val baseOffset = s"$input.getBaseOffset()"
    val numBytes = s"$input.numBytes()"
    s"$result = $hasherClassName.hashUnsafeBytes($baseObject, $baseOffset, $numBytes, $result);"
  }

  protected def genHashForMap(
                               ctx: CodegenContext,
                               input: String,
                               result: String,
                               keyType: DataType,
                               valueType: DataType,
                               valueContainsNull: Boolean): String = {
    val index = ctx.freshName("index")
    val keys = ctx.freshName("keys")
    val values = ctx.freshName("values")
    s"""
        final ArrayData $keys = $input.keyArray();
        final ArrayData $values = $input.valueArray();
        for (int $index = 0; $index < $input.numElements(); $index++) {
          ${nullSafeElementHash(keys, index, false, keyType, result, ctx)}
          ${nullSafeElementHash(values, index, valueContainsNull, valueType, result, ctx)}
        }
      """
  }

  protected def genHashForArray(
                                 ctx: CodegenContext,
                                 input: String,
                                 result: String,
                                 elementType: DataType,
                                 containsNull: Boolean): String = {
    val index = ctx.freshName("index")
    s"""
        for (int $index = 0; $index < $input.numElements(); $index++) {
          ${nullSafeElementHash(input, index, containsNull, elementType, result, ctx)}
        }
      """
  }

  protected def genHashForStruct(
                                  ctx: CodegenContext,
                                  input: String,
                                  result: String,
                                  fields: Array[StructField]): String = {
    val tmpInput = ctx.freshName("input")
    val fieldsHash = fields.zipWithIndex.map { case (field, index) =>
      nullSafeElementHash(tmpInput, index.toString, field.nullable, field.dataType, result, ctx)
    }
    val hashResultType = CodeGenerator.javaType(dataType)
    val code = ctx.splitExpressions(
      expressions = fieldsHash,
      funcName = "computeHashForStruct",
      arguments = Seq("InternalRow" -> tmpInput, hashResultType -> result),
      returnType = hashResultType,
      makeSplitFunction = body =>
        s"""
           |$body
           |return $result;
         """.stripMargin,
      foldFunctions = _.map(funcCall => s"$result = $funcCall;").mkString("\n"))
    s"""
       |final InternalRow $tmpInput = $input;
       |$code
     """.stripMargin
  }

  @tailrec
  private def computeHashWithTailRec(
                                      input: String,
                                      dataType: DataType,
                                      result: String,
                                      ctx: CodegenContext): String = dataType match {
    case NullType => ""
    case BooleanType => genHashBoolean(input, result)
    case ByteType | ShortType | IntegerType | DateType => genHashInt(input, result)
    case LongType => genHashLong(input, result)
    case TimestampType => genHashTimestamp(input, result)
    case FloatType => genHashFloat(input, result)
    case DoubleType => genHashDouble(input, result)
    case d: DecimalType => genHashDecimal(ctx, d, input, result)
    case CalendarIntervalType => genHashCalendarInterval(input, result)
      // TODO figure these out
//    case _: DayTimeIntervalType => genHashLong(input, result)
//    case YearMonthIntervalType => genHashInt(input, result)
    case BinaryType => genHashBytes(input, result)
    case StringType => genHashString(input, result)
    case ArrayType(et, containsNull) => genHashForArray(ctx, input, result, et, containsNull)
    case MapType(kt, vt, valueContainsNull) =>
      genHashForMap(ctx, input, result, kt, vt, valueContainsNull)
    case StructType(fields) => genHashForStruct(ctx, input, result, fields)
    case udt: UserDefinedType[_] => computeHashWithTailRec(input, udt.sqlType, result, ctx)
  }

  protected def computeHash(
                             input: String,
                             dataType: DataType,
                             result: String,
                             ctx: CodegenContext): String = computeHashWithTailRec(input, dataType, result, ctx)

  protected def hasherClassName: String
}

object SafeUTF8 {
  /**
   * Returns the actual byte array if it's a byte array, otherwise gets the bytes serialisation of it
   * @param s
   * @return
   */
  def safeUT8ByteArray(s: UTF8String): (Array[Byte], Int, Int) = {
    if (s.getBaseObject.isInstanceOf[Array[Byte]] && s.getBaseOffset >= Platform.BYTE_ARRAY_OFFSET.toLong) {
      val bytes = s.getBaseObject.asInstanceOf[Array[Byte]].asInstanceOf[Array[Byte]]
      val arrayOffset = s.getBaseOffset - Platform.BYTE_ARRAY_OFFSET.toLong
      if (bytes.length.toLong < arrayOffset + s.numBytes.toLong) throw new ArrayIndexOutOfBoundsException
      else
        (bytes, arrayOffset.toInt, s.numBytes)
    }
    else {
      (s.getBytes, 0, s.numBytes)
    }
  }
}

/**
 * Base class for interpreted hash functions.
 */
abstract class InterpretedHashLongsFunction {
  def hashInt(i: Int, digest: Digest): Digest

  def hashLong(l: Long, digest: Digest): Digest

  def hashBytes(base: Array[Byte], offset: Int, length: Int, digest: Digest): Digest

  /**
   * Computes hash of a given `value` of type `dataType`. The caller needs to check the validity
   * of input `value`.
   */
  def hash(value: Any, dataType: DataType, digest: Digest): Digest = {
    value match {
      case null => digest
      case b: Boolean => hashInt(if (b) 1 else 0, digest)
      case b: Byte => hashInt(b, digest)
      case s: Short => hashInt(s, digest)
      case i: Int => hashInt(i, digest)
      case l: Long => hashLong(l, digest)
      case f: Float if (f == -0.0f) => hashInt(0, digest)
      case f: Float => hashInt(java.lang.Float.floatToIntBits(f), digest)
      case d: Double if (d == -0.0d) => hashLong(0L, digest)
      case d: Double => hashLong(java.lang.Double.doubleToLongBits(d), digest)
      case d: Decimal =>
        val precision = dataType.asInstanceOf[DecimalType].precision
        if (precision <= Decimal.MAX_LONG_DIGITS) {
          hashLong(d.toUnscaledLong, digest)
        } else {
          val bytes = d.toJavaBigDecimal.unscaledValue().toByteArray
          hashBytes(bytes, Platform.BYTE_ARRAY_OFFSET, bytes.length, digest)
        }
      case c: CalendarInterval =>
        QualitySparkUtils.hashCalendarInterval(c, this, digest)
      case a: Array[Byte] =>
        hashBytes(a, Platform.BYTE_ARRAY_OFFSET, a.length, digest)
      case s: UTF8String => {
        /* */
        val (bytes, offset, numbytes) = SafeUTF8.safeUT8ByteArray(s)

        hashBytes(bytes, offset, numbytes, digest)

      }

      case array: ArrayData =>
        val elementType = dataType match {
          case udt: UserDefinedType[_] => udt.sqlType.asInstanceOf[ArrayType].elementType
          case ArrayType(et, _) => et
        }
        var result = digest
        var i = 0
        while (i < array.numElements()) {
          result = hash(array.get(i, elementType), elementType, result)
          i += 1
        }
        result

      case map: MapData =>
        val (kt, vt) = dataType match {
          case udt: UserDefinedType[_] =>
            val mapType = udt.sqlType.asInstanceOf[MapType]
            mapType.keyType -> mapType.valueType
          case MapType(kt, vt, _) => kt -> vt
        }
        val keys = map.keyArray()
        val values = map.valueArray()
        var result = digest
        var i = 0
        while (i < map.numElements()) {
          result = hash(keys.get(i, kt), kt, result)
          result = hash(values.get(i, vt), vt, result)
          i += 1
        }
        result

      case struct: InternalRow =>
        val types: Array[DataType] = dataType match {
          case udt: UserDefinedType[_] =>
            udt.sqlType.asInstanceOf[StructType].map(_.dataType).toArray
          case StructType(fields) => fields.map(_.dataType)
        }
        var result = digest
        var i = 0
        val len = struct.numFields
        while (i < len) {
          result = hash(struct.get(i, types(i)), types(i), result)
          i += 1
        }
        result
    }
  }
}
