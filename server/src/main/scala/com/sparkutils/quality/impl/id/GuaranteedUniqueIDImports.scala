package com.sparkutils.quality.impl.id

import org.apache.spark.sql.{Column, ShimUtils}
import org.apache.spark.sql.ShimUtils.{callFunction, column, expression}
import org.apache.spark.sql.functions.lit

trait GuaranteedUniqueIDImports {

  /**
   * Creates a uniqueID backed by the GuaranteedUniqueID Spark Snowflake ID approach
   */
  def unique_id(prefix: String): Column =
    callFunction("unique_id", lit(prefix))

  /**
   * Returns the size of an underlying ID, a unique_id will have 2, other id's may have more, each further increment is another 64bits
   * @param id
   * @return
   */
  def id_size(id: Column): Column =
    callFunction("id_size", id)

  /**
   * Converts either a single ID or individual field parts into base64.  The parts must be provided in the correct order, base, i0, i1.. iN
   * @param idFields
   * @return
   */
  def id_base64(idFields: Column *): Column =
    callFunction("id_base64", idFields:_*)

  /**
   * Given a base64 string convert to an ID, use id_size to understand how large IDs could be.
   * @param base64
   * @param size defaults to 2 (160bit ID)
   * @return
   */
  def id_from_base64(base64: Column, size: Int = 2): Column =
    callFunction("id_from_base64", base64, lit(size))

    /**
   * Returns the underlying raw type of an id (base, i0, i1 etc.) without prefixes
   * @param id
   * @return
   */
  def id_raw_type(id: Column): Column =
    callFunction("id_raw_type", id)
}
