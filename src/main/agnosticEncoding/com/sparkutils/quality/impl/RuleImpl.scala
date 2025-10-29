package com.sparkutils.quality.impl

import com.sparkutils.quality.sparkless.ProcessorFactory
import com.sparkutils.quality.sparkless.impl.Processors.processFactory
import com.sparkutils.quality.{LazyRuleSuiteResult, LazyRuleSuiteResultDetails, RuleSuiteResult, RuleSuiteResultDetails}
import org.apache.spark.sql.{Encoder, Row, ShimUtils}
import org.apache.spark.sql.types.StructType



/*
 * Need an approach via row, still classic runtime only
 */

object RowDeserializing {
  /**
   * Creates a processor which deserialises a Row into T, the Row **MUST** represent a T
   * @param rowEnc
   * @param toSize specifies the number of fields required to deserialize and create the [[T]]
   * @tparam T the output type
   * @return
   */
  protected[sparkutils] def rowDeserializerF[T: Encoder](rowEnc: Encoder[Row], toSize: Int): Row => T =
    rowDeserializer(rowEnc, toSize).instance // we know this is re-entrant safe

  /**
   * Creates a processor which deserialises a Row into T, the Row **MUST** represent a T
   * @param rowEnc
   * @param toSize specifies the number of fields required to deserialize and create the [[T]]
   * @tparam T the output type
   * @return
   */
  def rowDeserializer[T: Encoder](rowEnc: Encoder[Row], toSize: Int): ProcessorFactory[Row, T] = {
    implicit val i: Encoder[Row] = rowEnc
    processFactory[Row, T](identity, toSize)//( _ => implicitly[Encoder[T]])
  }
}

object LazyRuleSuiteResultDetailsUtils {

  lazy val deserializer = {
    RowDeserializing.rowDeserializerF(
      ShimUtils.rowEncoder(Encoders.ruleSuiteResultDetailsTypedEnc.catalystRepr.asInstanceOf[StructType]), 2)(
      Encoders.ruleSuiteResultDetailsExpEnc
    )
  }

}

case class LazyRuleSuiteResultDetailsImpl(row: Row) extends LazyRuleSuiteResultDetails with Serializable {
  @transient
  lazy val _ruleSuiteResultDetails = LazyRuleSuiteResultDetailsUtils.deserializer(row)

  override def ruleSuiteResultDetails: RuleSuiteResultDetails = _ruleSuiteResultDetails
}

object LazyRuleSuiteResultUtils {

  lazy val deserializer = {
    RowDeserializing.rowDeserializerF(
      ShimUtils.rowEncoder(Encoders.ruleSuiteResultTypedEnc.catalystRepr.asInstanceOf[StructType]), 3)(
      Encoders.ruleSuiteResultExpEnc
    )
  }

}

case class LazyRuleSuiteResultImpl(row: Row) extends LazyRuleSuiteResult with Serializable {
  @transient
  lazy val _ruleSuiteResult = LazyRuleSuiteResultUtils.deserializer(row)

  override val ruleSuiteResult: RuleSuiteResult = _ruleSuiteResult
}