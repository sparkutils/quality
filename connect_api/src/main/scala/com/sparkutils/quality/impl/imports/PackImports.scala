package com.sparkutils.quality.impl.imports

import com.sparkutils.quality.Id
import org.apache.spark.sql.ShimUtils.callFunction
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

trait PackIdImports {

  /**
   * Packs two integers into a long, typically used for versioned ids.
   * @param id
   * @param version
   * @return
   */
  def pack_ints(id: Column, version: Column): Column = callFunction("pack_ints", id, version)

  /**
   * Packs two integers into a long, typically used for versioned ids.
   *
   * @param id
   * @param version
   * @return
   */
  def pack_ints(id: Int, version: Int): Column = pack_ints(lit(id), lit(version))

  /**
   * Packs two integers into a long, typically used for versioned ids.
   *
   * @param id
   * @return
   */
  def pack_ints(id: Id): Column = pack_ints(lit(id.id), lit(id.version))

  /**
   * Takes a packedId long and unpacks to id, version
   * @param packedIs
   * @return
   */
  def unpack(packedId: Column): Column = callFunction("unpack", packedId)

  /**
   * Unpacks an IdTriple column into it's six constituent integers
   * @param idTriple
   * @return
   */
  def unpack_id_triple(idTriple: Column): Column = callFunction("unpack_Id_triple", idTriple)
}
