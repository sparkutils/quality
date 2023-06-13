package com.sparkutils.quality.impl

import org.apache.spark.sql.types._

trait RuleSparkTypes {

  val ruleResultType = IntegerType
  val packedIdType = LongType
  val overallResultType = StructField("overallResult", ruleResultType, false)
  val ruleSetType = StructType(Seq(overallResultType, StructField("ruleResults", MapType(packedIdType, ruleResultType, false))))
  val ruleSetResultsType = MapType(packedIdType, ruleSetType, false)
  val ruleSetsType = StructField("ruleSetResults", ruleSetResultsType)
  val ruleSuiteResultType = StructType(Seq(StructField("id", packedIdType), overallResultType, ruleSetsType ))
  val fullRuleIdType = StructType(Seq(StructField("ruleSuiteId", packedIdType), StructField("ruleSetId", packedIdType), StructField("ruleId", packedIdType)))

  val ruleSuiteDetailsResultType = StructType(Seq(StructField("id", packedIdType), ruleSetsType ))
}
