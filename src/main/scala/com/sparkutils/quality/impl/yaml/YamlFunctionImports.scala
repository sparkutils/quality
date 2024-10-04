package com.sparkutils.quality.impl.yaml

import org.apache.spark.sql.Column
import org.apache.spark.sql.ShimUtils.{column, expression}
import org.apache.spark.sql.types.DataType

trait YamlFunctionImports {

  /**
   * Converts spark expressions to yaml using snakeyml.
   *
   * @param col
   * @return
   */
  def to_yaml(col: Column, renderOptions: Map[String, String] = Map.empty): Column =
    column(YamlEncoderExpr(expression(col), renderOptions))

  /**
   * Converts yaml expressions to spark native types
   *
   * @param yaml
   * @param dataType the yaml's data type
   * @return
   */
  def from_yaml(yaml: Column, dataType: DataType): Column =
    column(YamlDecoderExpr(expression(yaml), dataType))

}
