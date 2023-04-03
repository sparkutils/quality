package com.sparkutils.quality.impl.extension

import org.apache.spark.sql.{QualitySparkUtils, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.qualityFunctions.utils
import org.apache.spark.sql.types.DataType

/**
 * Registers Quality sql functions using the defaults for registerQualityFunctions, these can be overridden without having to subclass DriverPlugin
 */
class QualitySparkExtension extends ((SparkSessionExtensions) => Unit) {

  def parseTypes: String => Option[DataType] = com.sparkutils.quality.defaultParseTypes _
  def zero: DataType => Option[Any] = com.sparkutils.quality.defaultZero _
  def add: DataType => Option[(Expression, Expression) => Expression] = (dataType: DataType) => com.sparkutils.quality.defaultAdd(dataType)
  def mapCompare: DataType => Option[(Any, Any) => Int] = (dataType: DataType) => utils.defaultMapCompare(dataType)
  def writer: String => Unit = println(_)

  override def apply(extensions: SparkSessionExtensions): Unit = {
    com.sparkutils.quality.registerQualityFunctions(parseTypes, zero, add, mapCompare, writer,
      register = QualitySparkUtils.registerFunctionViaExtension(extensions) _
    )
  }

}