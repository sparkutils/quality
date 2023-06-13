package org.apache.spark.sql.catalyst.types

import org.apache.spark.sql.types.DataType

object PhysicalDataType {
  // https://issues.apache.org/jira/browse/SPARK-43019 in 3.5, backported to 13.1 dbr
  def ordering(dataType: DataType): Ordering[_] = ???
}