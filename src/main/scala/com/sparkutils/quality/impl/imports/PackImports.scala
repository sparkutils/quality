package com.sparkutils.quality.impl.imports

import com.sparkutils.quality.Id
import com.sparkutils.quality.impl.{Pack, UnPack, UnPackIdTriple}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

trait PackIdImports {

  /**
   * Packs two integers into a long, typically used for versioned ids.
   * @param id
   * @param version
   * @return
   */
  def pack_ints(id: Column, version: Column): Column = Pack.apply(id, version)

  /**
   * Packs two integers into a long, typically used for versioned ids.
   *
   * @param id
   * @param version
   * @return
   */
  def pack_ints(id: Int, version: Int): Column = Pack.apply(lit(id), lit(version))

  /**
   * Packs two integers into a long, typically used for versioned ids.
   *
   * @param id
   * @return
   */
  def pack_ints(id: Id): Column = Pack.apply(lit(id.id), lit(id.version))

  /**
   * Takes a packedId long and unpacks to id, version
   * @param packedIs
   * @return
   */
  def unpack(packedId: Column): Column = UnPack.apply(packedId)

  /**
   * Unpacks an IdTriple column into it's six constituent integers
   * @param idTriple
   * @return
   */
  def unpack_id_triple(idTriple: Column): Column = UnPackIdTriple.apply(idTriple)
}
