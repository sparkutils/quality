package com.sparkutils.quality.impl.yaml

import org.apache.spark.sql.Column
import org.apache.spark.sql.types.DataType

trait YamlFunctionImports {

  /**
   * Converts spark expressions to yaml using snakeyml.
   *
   * @param col
   * @return
   */
  def to_yaml(col: Column, renderOptions: Map[String, String] = Map.empty): Column =
    new Column(YamlEncoderExpr(col.expr, renderOptions))

  /**
   * Converts yaml expressions to spark native types
   *
   * @param yaml
   * @param dataType the yaml's data type
   * @return
   */
  def from_yaml(yaml: Column, dataType: DataType): Column =
    new Column(YamlDecoderExpr(yaml.expr, dataType))

}
