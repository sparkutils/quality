package org.apache.spark.sql.qualityFunctions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeConstants.MICROS_PER_MILLIS
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{fromJavaDate, fromJavaTimestamp, localDateTimeToMicros}
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, GenericArrayData}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, DayTimeIntervalType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, Metadata, NullType, ShortType, StringType, StructType, TimestampNTZType, TimestampType, YearMonthIntervalType}
import org.apache.spark.unsafe.types.UTF8String

import java.math.{BigDecimal => JBigDecimal}
import java.nio.charset.StandardCharsets
import java.sql.{Connection, Date, ResultSet, Time, Timestamp}

package object jdbc {

  type JDBCValueGetter = (ResultSet, InternalRow, Int) => Unit


  def jdbcHelper(jdbcURL: String,
                 conn: Connection,
                 resultSet: ResultSet,
                 alwaysNullable: Boolean = false,
                 isTimestampNTZ: Boolean = false): JdbcHelper =  {
    val dialect = JdbcDialects.get(jdbcURL)
    val schema: StructType = JdbcUtils.getSchema(conn, resultSet, dialect, alwaysNullable, isTimestampNTZ)
    jdbcHelper(dialect, schema)
  }

  def jdbcHelper(dialect: JdbcDialect,
                 schema: StructType): JdbcHelper =
    new JdbcHelper(schema, makeGetters(dialect, schema))

  /**
   * Creates `JDBCValueGetter`s according to [[StructType]], which can set
   * each value from `ResultSet` to each field of [[InternalRow]] correctly.
   *
   * The `makeGetters` and `makeGetter` functions are copied from spark repository
   * https://github.com/apache/spark/blob/47be8822902ed2e9a218367bceea6809a9af50e4/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JdbcUtils.scala#L398
   */
  private def makeGetters(
                           dialect: JdbcDialect,
                           schema: StructType) = {
    val replaced = CharVarcharUtils.replaceCharVarcharWithStringInSchema(schema)
    replaced.fields.map(sf => sf.dataType -> makeGetter(sf.dataType, dialect, sf.metadata))
  }

  private def makeGetter(
                          dt: DataType,
                          dialect: JdbcDialect,
                          metadata: Metadata): JDBCValueGetter = dt match {
    case BooleanType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setBoolean(pos, rs.getBoolean(pos + 1))

    case DateType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        // DateTimeUtils.fromJavaDate does not handle null value, so we need to check it.
        val dateVal = rs.getDate(pos + 1)
        if (dateVal != null) {
          row.setInt(pos, fromJavaDate(dialect.convertJavaDateToDate(dateVal)))
        } else {
          row.update(pos, null)
        }

    // When connecting with Oracle DB through JDBC, the precision and scale of BigDecimal
    // object returned by ResultSet.getBigDecimal is not correctly matched to the table
    // schema reported by ResultSetMetaData.getPrecision and ResultSetMetaData.getScale.
    // If inserting values like 19999 into a column with NUMBER(12, 2) type, you get through
    // a BigDecimal object with scale as 0. But the dataframe schema has correct type as
    // DecimalType(12, 2). Thus, after saving the dataframe into parquet file and then
    // retrieve it, you will get wrong result 199.99.
    // So it is needed to set precision and scale for Decimal based on JDBC metadata.
    case DecimalType.Fixed(p, s) =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val decimal =
          nullSafeConvert[JBigDecimal](rs.getBigDecimal(pos + 1), d => Decimal(d, p, s))
        row.update(pos, decimal)

    case DoubleType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setDouble(pos, rs.getDouble(pos + 1))

    case FloatType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setFloat(pos, rs.getFloat(pos + 1))

    case IntegerType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setInt(pos, rs.getInt(pos + 1))

    case LongType if metadata.contains("binarylong") =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val l = nullSafeConvert[Array[Byte]](rs.getBytes(pos + 1), bytes => {
          var ans = 0L
          var j = 0
          while (j < bytes.length) {
            ans = 256 * ans + (255 & bytes(j))
            j = j + 1
          }
          ans
        })
        row.update(pos, l)

    case LongType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setLong(pos, rs.getLong(pos + 1))

    case ShortType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setShort(pos, rs.getShort(pos + 1))

    case ByteType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setByte(pos, rs.getByte(pos + 1))

    case StringType if metadata.contains("rowid") =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val rawRowId = rs.getRowId(pos + 1)
        if (rawRowId == null) {
          row.update(pos, null)
        } else {
          row.update(pos, UTF8String.fromString(rawRowId.toString))
        }

    case StringType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        // TODO(davies): use getBytes for better performance, if the encoding is UTF-8
        row.update(pos, UTF8String.fromString(rs.getString(pos + 1)))

    // SPARK-34357 - sql TIME type represents as zero epoch timestamp.
    // It is mapped as Spark TimestampType but fixed at 1970-01-01 for day,
    // time portion is time of day, with no reference to a particular calendar,
    // time zone or date, with a precision till microseconds.
    // It stores the number of milliseconds after midnight, 00:00:00.000000
    case TimestampType if metadata.contains("logical_time_type") =>
      (rs: ResultSet, row: InternalRow, pos: Int) => {
        row.update(pos, nullSafeConvert[Time](
          rs.getTime(pos + 1), t => Math.multiplyExact(t.getTime, MICROS_PER_MILLIS)))
      }

    case TimestampType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val t = rs.getTimestamp(pos + 1)
        if (t != null) {
          row.setLong(pos, fromJavaTimestamp(dialect.convertJavaTimestampToTimestamp(t)))
        } else {
          row.update(pos, null)
        }

    case TimestampNTZType if metadata.contains("logical_time_type") =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val micros = nullSafeConvert[Time](rs.getTime(pos + 1), t => {
          val time = dialect.convertJavaTimestampToTimestampNTZ(new Timestamp(t.getTime))
          localDateTimeToMicros(time)
        })
        row.update(pos, micros)

    case TimestampNTZType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val t = rs.getTimestamp(pos + 1)
        if (t != null) {
          row.setLong(pos, localDateTimeToMicros(dialect.convertJavaTimestampToTimestampNTZ(t)))
        } else {
          row.update(pos, null)
        }

    case BinaryType if metadata.contains("binarylong") =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val bytes = rs.getBytes(pos + 1)
        if (bytes != null) {
          val binary = bytes.flatMap(Integer.toBinaryString(_).getBytes(StandardCharsets.US_ASCII))
          row.update(pos, binary)
        } else {
          row.update(pos, null)
        }

    case BinaryType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.update(pos, rs.getBytes(pos + 1))

    case _: YearMonthIntervalType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.update(pos,
          nullSafeConvert(rs.getString(pos + 1), dialect.getYearMonthIntervalAsMonths))

    case _: DayTimeIntervalType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.update(pos,
          nullSafeConvert(rs.getString(pos + 1), dialect.getDayTimeIntervalAsMicros))

    case _: ArrayType if metadata.contains("pg_bit_array_type") =>
      // SPARK-47628: Handle PostgreSQL bit(n>1) array type ahead. As in the pgjdbc driver,
      // bit(n>1)[] is not distinguishable from bit(1)[], and they are all recognized as boolen[].
      // This is wrong for bit(n>1)[], so we need to handle it first as byte array.
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val fieldString = rs.getString(pos + 1)
        if (fieldString != null) {
          val strArray = fieldString.substring(1, fieldString.length - 1).split(",")
          // Charset is picked from the pgjdbc driver for consistency.
          val bytesArray = strArray.map(_.getBytes(StandardCharsets.US_ASCII))
          row.update(pos, new GenericArrayData(bytesArray))
        } else {
          row.update(pos, null)
        }

    case ArrayType(et, _) =>
      def elementConversion(et: DataType): AnyRef => Any = et match {
        case TimestampType => arrayConverter[Timestamp] {
          (t: Timestamp) => fromJavaTimestamp(dialect.convertJavaTimestampToTimestamp(t))
        }

        case TimestampNTZType =>
          arrayConverter[Timestamp] {
            (t: Timestamp) => localDateTimeToMicros(dialect.convertJavaTimestampToTimestampNTZ(t))
          }

        case StringType =>
          arrayConverter[Object]((obj: Object) => UTF8String.fromString(obj.toString))

        case DateType => arrayConverter[Date] {
          (d: Date) => fromJavaDate(dialect.convertJavaDateToDate(d))
        }

        case dt: DecimalType =>
          arrayConverter[java.math.BigDecimal](d => Decimal(d, dt.precision, dt.scale))

        case LongType if metadata.contains("binarylong") =>
          throw QueryExecutionErrors.unsupportedArrayElementTypeBasedOnBinaryError(dt)

        case ArrayType(et0, _) =>
          arrayConverter[Array[Any]] {
            arr => new GenericArrayData(elementConversion(et0)(arr))
          }

        case IntegerType => arrayConverter[Int]((i: Int) => i)
        case FloatType => arrayConverter[Float]((f: Float) => f)
        case DoubleType => arrayConverter[Double]((d: Double) => d)
        case ShortType => arrayConverter[Short]((s: Short) => s)
        case BooleanType => arrayConverter[Boolean]((b: Boolean) => b)
        case LongType => arrayConverter[Long]((l: Long) => l)

        case _ => (array: Object) => array.asInstanceOf[Array[Any]]
      }

      (rs: ResultSet, row: InternalRow, pos: Int) =>
        try {
          val array = nullSafeConvert[java.sql.Array](
            input = rs.getArray(pos + 1),
            array => new GenericArrayData(elementConversion(et)(array.getArray())))
          row.update(pos, array)
        } catch {
          case e: java.lang.ClassCastException =>
            throw QueryExecutionErrors.wrongDatatypeInSomeRows(pos, dt)
        }

    case NullType =>
      (_: ResultSet, row: InternalRow, pos: Int) => row.update(pos, null)

    case _ => throw QueryExecutionErrors.unsupportedJdbcTypeError(dt.catalogString)
  }

  private def nullSafeConvert[T](input: T, f: T => Any): Any = {
    if (input == null) {
      null
    } else {
      f(input)
    }
  }

  private def arrayConverter[T](elementConvert: T => Any): Any => Any = (array: Any) => {
    array.asInstanceOf[Array[T]].map(e => nullSafeConvert(e, elementConvert))
  }
}
