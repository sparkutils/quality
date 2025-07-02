package org.apache.spark.sql.qualityFunctions.jdbc

import com.sparkutils.quality.sparkless.{ProcessorFactory, ProcessorFactoryInputProxyWithState}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.types.{DataType, StructType}

import java.sql.ResultSet

class JdbcHelper private[jdbc] (schema: StructType, getters: Seq[(DataType, JDBCValueGetter)]) {

  def wrapResultSet[O](processorFactory: ProcessorFactory[Row, O]): ProcessorFactory[ResultSet, O] =
    new ProcessorFactoryInputProxyWithState(
      underlyingFactory = processorFactory,
      stateConstructor = () => new SpecificInternalRow(getters.map(_._1)),
      convert = convertResultSetToRow)


  /**
   * Fills targetRow from resultSet, it does not make any attempt to check if there is a row, callers are responsible for
   * ResultSet next management
   */
  private def process(targetRow: InternalRow, resultSet: ResultSet): Unit = {
    var i = 0
    while (i < getters.length) {
      getters(i)._2.apply(resultSet, targetRow, i)
      if (resultSet.wasNull) targetRow.setNullAt(i)
      i = i + 1
    }
  }

  private def convertResultSetToRow(internalRow: InternalRow, resultSet: ResultSet): Row = {
    process(internalRow, resultSet)
    val fromRow = ExpressionEncoder(RowEncoder.encoderFor(schema, lenient = false)).resolveAndBind().createDeserializer()
    fromRow(internalRow)
  }
}