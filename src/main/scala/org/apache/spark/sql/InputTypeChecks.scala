package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.ExpectsInputTypes
import org.apache.spark.sql.types.{AbstractDataType, DataType, TypeCollection}

/**
 * Provides a simpler wrapper around types
 */
trait InputTypeChecks extends ExpectsInputTypes {

  def inputDataTypes: Seq[Seq[DataType]]

  def inputTypes: Seq[AbstractDataType] = inputDataTypes.map{
    types =>

      if (types.size > 1)
        TypeCollection(types : _*)
      else
        types.head
  }
}
