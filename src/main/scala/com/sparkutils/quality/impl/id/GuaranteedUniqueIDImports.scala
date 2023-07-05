package com.sparkutils.quality.impl.id

import org.apache.spark.sql.Column

trait GuaranteedUniqueIDImports {
  /**
   * Creates a uniqueID backed by the GuaranteedUniqueID Spark Snowflake ID approach, in line with the sql function naming, please migrate to unique_id
   */
  @deprecated(since="0.1.0", message="migrate to unique_id")
  def uniqueID(prefix: String): Column =
    new Column(GuaranteedUniqueIdIDExpression(
      GuaranteedUniqueID() // defaults are all fine, ms just relates to definition instead of action
      , prefix
    ))

  /**
   * Creates a uniqueID backed by the GuaranteedUniqueID Spark Snowflake ID approach
   */
  def unique_id(prefix: String): Column =
    new Column(GuaranteedUniqueIdIDExpression(
      GuaranteedUniqueID() // defaults are all fine, ms just relates to definition instead of action
      , prefix
    ))

  /**
   * Returns the size of an underlying ID, a unique_id will have 2, other id's may have more, each further increment is another 64bits
   * @param id
   * @return
   */
  def id_size(id: Column): Column = new Column( SizeOfIDString(id.expr) )

  /**
   * Converts either a single ID or individual field parts into base64.  The parts must be provided in the correct order, base, i0, i1.. iN
   * @param idFields
   * @return
   */
  def id_base64(idFields: Column *): Column = new Column( idFields match {
    case Seq(e) => AsBase64Struct(e.expr)
    case _ => AsBase64Fields(idFields.map(_.expr))
  } )

  /**
   * Given a base64 string convert to an ID, use id_size to understand how large IDs could be.
   * @param base64
   * @param size defaults to 2 (160bit ID)
   * @return
   */
  def id_from_base64(base64: Column, size: Int = 2): Column = new Column( IDFromBase64(base64.expr, size) )

  /**
   * Returns the underlying raw type of an id (base, i0, i1 etc.) without prefixes
   * @param id
   * @return
   */
  def id_raw_type(id: Column): Column = new Column( IDToRawIDDataType(id.expr) )
}
