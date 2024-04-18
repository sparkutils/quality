package com.sparkutils.quality.impl.util

import org.apache.spark.sql.{Column, QualitySparkUtils}

trait StructFunctionsImport {

  /**
   * Adds fields, in order, for each field path it's paired transformation is applied to the update column
   *
   * @param update
   * @param transformations
   * @return a new copy of update with the changes applied
   */
  def update_field(update: Column, transformations: (String, Column)*): Column =
    QualitySparkUtils.update_field(update, transformations :_*)

  /**
   * Drops a field from a structure
   * @param update
   * @param fieldNames may be nested
   * @return
   */
  def drop_field(update: Column, fieldNames: String*): Column =
    QualitySparkUtils.drop_field(update, fieldNames :_*)
}