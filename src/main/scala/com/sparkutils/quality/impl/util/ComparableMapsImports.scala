package com.sparkutils.quality.impl.util

import org.apache.spark.sql.Column
import org.apache.spark.sql.qualityFunctions.utils
import org.apache.spark.sql.types.DataType

trait ComparableMapsImports {
  def comparableMaps(child: Column, compareF: DataType => Option[(Any, Any) => Int] = (dataType: DataType) => utils.defaultMapCompare(dataType)): Column =
    ComparableMapConverter(child,compareF)

  def reverseComparableMaps(child: Column): Column =
    new Column(ComparableMapReverser(child.expr))
}
