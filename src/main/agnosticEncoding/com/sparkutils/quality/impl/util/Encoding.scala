package com.sparkutils.quality.impl.util

import frameless.TypedEncoder
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.{AgnosticEncoder, ExpressionEncoder}

object Encoding {

  /**
   * Wraps a non-Frameless encoder in a TypedEncoder, adjusting paths as needed.
   *
   * This is not intended for general use and is used by the ProcessFunctions.
   *
   * @param outputType
   * @tparam T
   * @return
   */
  implicit def fromNormalEncoder[T: Encoder]: TypedEncoder[T] = {

    val (clt, agEnc) =
      implicitly[Encoder[T]] match {
        case e: ExpressionEncoder[T] => (e.clsTag, e.encoder)
        case a: AgnosticEncoder[T] => (a.clsTag, a)
      }

    implicit val classTag = clt

    new TypedEncoder[T]() {
      override def nullable: Boolean = agnosticEncoder.nullable
      override val agnosticEncoder: AgnosticEncoder[T] =
        agEnc
    }
  }

}

