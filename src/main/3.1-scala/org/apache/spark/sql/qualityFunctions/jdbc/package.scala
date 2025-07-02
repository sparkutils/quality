package org.apache.spark.sql.qualityFunctions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, GenericArrayData}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.sql.{Connection, ResultSet}
import java.util.concurrent.TimeUnit

package object jdbc {

  type JDBCValueGetter = (ResultSet, InternalRow, Int) => Unit


  def jdbcHelper(jdbcURL: String,
                 conn: Connection,
                 resultSet: ResultSet,
                 alwaysNullable: Boolean = false,
                 isTimestampNTZ: Boolean = false): JdbcHelper =  {
    val dialect = JdbcDialects.get(jdbcURL)
    val schema: StructType = JdbcUtils.getSchema(resultSet, dialect, alwaysNullable)
    jdbcHelper(dialect, schema)
  }

  def jdbcHelper(dialect: JdbcDialect,
                 schema: StructType): JdbcHelper =
    new JdbcHelper(schema, makeGetters(schema))



  /**
   * Creates `JDBCValueGetter`s according to [[StructType]], which can set
   * each value from `ResultSet` to each field of [[InternalRow]] correctly.
   * This is a copy from the spark repository
   * https://github.com/apache/spark/blob/v3.1.3/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JdbcUtils.scala#L374
   */
  private def makeGetters(schema: StructType) =
    schema.fields.map(sf => sf.dataType -> makeGetter(sf.dataType, sf.metadata)).toIndexedSeq

  private def makeGetter(dt: DataType, metadata: Metadata): JDBCValueGetter = dt match {
    case BooleanType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setBoolean(pos, rs.getBoolean(pos + 1))

    case DateType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        // DateTimeUtils.fromJavaDate does not handle null value, so we need to check it.
        val dateVal = rs.getDate(pos + 1)
        if (dateVal != null) {
          row.setInt(pos, DateTimeUtils.fromJavaDate(dateVal))
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
          nullSafeConvert[java.math.BigDecimal](rs.getBigDecimal(pos + 1), d => Decimal(d, p, s))
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
        val bytes = rs.getBytes(pos + 1)
        var ans = 0L
        var j = 0
        while (j < bytes.length) {
          ans = 256 * ans + (255 & bytes(j))
          j = j + 1
        }
        row.setLong(pos, ans)

    case LongType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setLong(pos, rs.getLong(pos + 1))

    case ShortType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setShort(pos, rs.getShort(pos + 1))

    case ByteType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.setByte(pos, rs.getByte(pos + 1))

    case StringType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        // TODO(davies): use getBytes for better performance, if the encoding is UTF-8
        row.update(pos, UTF8String.fromString(rs.getString(pos + 1)))

    case TimestampType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val t = rs.getTimestamp(pos + 1)
        if (t != null) {
          row.setLong(pos, DateTimeUtils.fromJavaTimestamp(t))
        } else {
          row.update(pos, null)
        }

    case BinaryType =>
      (rs: ResultSet, row: InternalRow, pos: Int) =>
        row.update(pos, rs.getBytes(pos + 1))

    case ArrayType(et, _) =>
      val elementConversion = et match {
        case TimestampType =>
          (array: Object) =>
            array.asInstanceOf[Array[java.sql.Timestamp]].map { timestamp =>
              nullSafeConvert(timestamp, DateTimeUtils.fromJavaTimestamp)
            }

        case StringType =>
          (array: Object) =>
            // some underling types are not String such as uuid, inet, cidr, etc.
            array.asInstanceOf[Array[java.lang.Object]]
              .map(obj => if (obj == null) null else UTF8String.fromString(obj.toString))

        case DateType =>
          (array: Object) =>
            array.asInstanceOf[Array[java.sql.Date]].map { date =>
              nullSafeConvert(date, DateTimeUtils.fromJavaDate)
            }

        case dt: DecimalType =>
          (array: Object) =>
            array.asInstanceOf[Array[java.math.BigDecimal]].map { decimal =>
              nullSafeConvert[java.math.BigDecimal](
                decimal, d => Decimal(d, dt.precision, dt.scale))
            }

        case LongType if metadata.contains("binarylong") =>
          throw new IllegalArgumentException(s"Unsupported array element " +
            s"type ${dt.catalogString} based on binary")

        case ArrayType(_, _) =>
          throw new IllegalArgumentException("Nested arrays unsupported")

        case _ => (array: Object) => array.asInstanceOf[Array[Any]]
      }

      (rs: ResultSet, row: InternalRow, pos: Int) =>
        val array = nullSafeConvert[java.sql.Array](
          input = rs.getArray(pos + 1),
          array => new GenericArrayData(elementConversion.apply(array.getArray)))
        row.update(pos, array)

    case _ => throw new IllegalArgumentException(s"Unsupported type ${dt.catalogString}")
  }

  private def nullSafeConvert[T](input: T, f: T => Any): Any = {
    if (input == null) {
      null
    } else {
      f(input)
    }
  }
}
