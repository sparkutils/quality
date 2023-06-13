package com.sparkutils.quality.impl.id

import com.sparkutils.quality.impl.hash.{HashFunctionFactory, HashFunctionsExpression, MessageDigestFactory}
import com.sparkutils.quality.impl.rng.RandLongsWithJump
import org.apache.commons.rng.simple.RandomSource
import org.apache.spark.sql.Column
import org.apache.spark.sql.qualityFunctions.DigestFactory

trait GenericLongBasedImports {
  /**
   * Creates a default randomRNG based on RandomSource.XO_RO_SHI_RO_128_PP
   */
  def rngID(prefix: String): Column =
    new Column(GenericLongBasedIDExpression(model.RandomID,
      RandLongsWithJump(0L, RandomSource.XO_RO_SHI_RO_128_PP), prefix))

  /**
   * Creates a hash based ID based on a 128 bit MD5 by default
   * @param prefix
   * @return
   */
  def fieldBasedID(prefix: String, children: Seq[Column], digestImpl: String = "MD5", digestFactory: String => DigestFactory = MessageDigestFactory): Column =
    new Column(GenericLongBasedIDExpression(model.FieldBasedID,
      HashFunctionsExpression(children.map(_.expr), digestImpl, true, digestFactory(digestImpl)), prefix))

  def fieldBasedID(prefix: String, digestImpl: String, children: Column *): Column =
    fieldBasedID(prefix, children, digestImpl)

  /**
   * Creates a hash based ID based on an upstream compatible long generator
   * @param prefix
   * @return
   */
  def providedID(prefix: String, child: Column): Column =
    new Column(GenericLongBasedIDExpression(model.ProvidedID, child.expr, prefix))

  /**
   * Murmur3 hash
   * @param prefix
   * @param children
   * @param digestImpl - only Murmur3 currently supported
   * @return
   */
  def hashID(prefix: String, children: Seq[Column], digestImpl: String = "IGNORED"): Column =
    new Column(GenericLongBasedIDExpression(model.FieldBasedID,
      HashFunctionsExpression(children.map(_.expr), digestImpl, true, HashFunctionFactory("IGNORED")), prefix))

  def hashID(prefix: String, digestImpl: String, children: Column*): Column = hashID(prefix, children, digestImpl)

  /**
   * Murmur3 hash
   * @param prefix
   * @param children
   * @return
   */
  def murmur3ID(prefix: String, children: Seq[Column]): Column = hashID(prefix, children, "M3_128")
  def murmur3ID(prefix: String, child1: Column, restOfchildren: Column*): Column = hashID(prefix, child1 +: restOfchildren, "M3_128")

}
