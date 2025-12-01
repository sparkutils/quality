package com.sparkutils.quality.impl

import com.sparkutils.quality.{LazyRuleSuiteResult, LazyRuleSuiteResultDetails, RuleSuiteResult, RuleSuiteResultDetails}
import org.apache.spark.sql.ShimUtils
import org.apache.spark.sql.catalyst.InternalRow

/*
 * InternalRow manipulation isn't possible with AgnosticEncoding
 */
object LazyRuleSuiteResultDetailsUtils {
  lazy val deserializer = ShimUtils.expressionEncoder( Encoders.ruleSuiteResultDetailsExpEnc ).
    resolveAndBind().deserializer
}

case class LazyRuleSuiteResultDetailsImpl(row: InternalRow) extends LazyRuleSuiteResultDetails with Serializable {
  @transient
  lazy val _ruleSuiteResultDetails = LazyRuleSuiteResultDetailsUtils.deserializer.eval(row).
    asInstanceOf[RuleSuiteResultDetails]

  override def ruleSuiteResultDetails: RuleSuiteResultDetails = _ruleSuiteResultDetails
}

object LazyRuleSuiteResultUtils {
  lazy val deserializer = ShimUtils.expressionEncoder( Encoders.ruleSuiteResultExpEnc ).
    resolveAndBind().deserializer
}

case class LazyRuleSuiteResultImpl(row: InternalRow) extends LazyRuleSuiteResult with Serializable {
  @transient
  lazy val _ruleSuiteResult = LazyRuleSuiteResultUtils.deserializer.eval(row).
    asInstanceOf[RuleSuiteResult]

  override def ruleSuiteResult: RuleSuiteResult = _ruleSuiteResult
}