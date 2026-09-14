package com.sparkutils.quality.impl.aggregates

import com.sparkutils.quality.ResultStatisticsProvider.ResultStatisticOps
import com.sparkutils.quality.{RuleSuiteGroupStatistics, RuleSuiteResult}
import org.apache.spark.sql.{ClassicQualitySparkUtils, Encoder}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.expressions.Aggregator

/**
 * Processes RuleSuiteResult's into RuleSuiteGroupStatistics, as of 0.1.4 using UDAF Aggregator
 */
object Statistics extends Aggregator[Tuple1[RuleSuiteResult], RuleSuiteGroupStatistics, RuleSuiteGroupStatistics] with Serializable {

  def apply(exp: Expression): Expression = {
    import com.sparkutils.quality.impl.Encoders.ruleSuiteResultTypedEnc
    import frameless._
    implicit val enc = TypedExpressionEncoder[Tuple1[RuleSuiteResult]]
    ClassicQualitySparkUtils.aggregator(this, Seq(exp))
  }

  override def zero: RuleSuiteGroupStatistics = RuleSuiteGroupStatistics()

  override def reduce(b: RuleSuiteGroupStatistics, a: Tuple1[RuleSuiteResult]): RuleSuiteGroupStatistics =
    b.process(a._1)

  override def merge(b1: RuleSuiteGroupStatistics, b2: RuleSuiteGroupStatistics): RuleSuiteGroupStatistics =
    b1.combine(b2)

  override def finish(reduction: RuleSuiteGroupStatistics): RuleSuiteGroupStatistics = reduction

  override def bufferEncoder: Encoder[RuleSuiteGroupStatistics] =
    com.sparkutils.quality.impl.Encoders.ruleSuiteGroupStatisticsTypedExpEnc

  override def outputEncoder: Encoder[RuleSuiteGroupStatistics] =
    com.sparkutils.quality.impl.Encoders.ruleSuiteGroupStatisticsTypedExpEnc

}