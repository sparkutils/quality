package com.sparkutils.quality.impl

import org.apache.spark.sql.types._

trait RuleSparkTypes {

  /** DQ Result Type */
  val ruleResultType: DataType = IntegerType
  /** The type of a stored Id/VersionedId */
  val packedIdType: DataType = LongType
  /** The field storing overallResults in both ruleSetType and ruleSuiteResultsType */
  val overallResultType: StructField = StructField("overallResult", ruleResultType, false)
  /** The results of a ruleSets evaluation */
  val ruleSetType: StructType = StructType(Seq(overallResultType, StructField("ruleResults", MapType(packedIdType, ruleResultType, false))))
  /** ruleSetType with the ruleSet's packed id */
  val ruleSetResultsType: MapType = MapType(packedIdType, ruleSetType, false)
  /** The field for ruleSetResultsType */
  val ruleSetsType: StructField = StructField("ruleSetResults", ruleSetResultsType)
  /** Top level result for a ruleRunner, with overallResults present, containing all ruleSet results */
  val ruleSuiteResultType: StructType = StructType(Seq(StructField("id", packedIdType), overallResultType, ruleSetsType ))
  /** Triple of packed ruleSuiteId, ruleSetId, ruleId */
  val fullRuleIdType: StructType = StructType(Seq(StructField("ruleSuiteId", packedIdType), StructField("ruleSetId", packedIdType), StructField("ruleId", packedIdType)))

  /** Unlike ruleSuiteResultType the suite's overallResultType is not stored in the results */
  val ruleSuiteDetailsResultType: StructType = StructType(Seq(StructField("id", packedIdType), ruleSetsType ))

  /** Unlike DQ results the results of the expression are cast to string, with the original DDL */
  val expressionResultType: StructType = StructType(Seq(StructField("result", StringType), StructField("resultDDL", StringType)))
  /** A given ruleSet's results of packedId's expressionResultType, with the original DDL */
  val expressionsRuleSetType: MapType = MapType(packedIdType, expressionResultType)
  /** The collection of expression ruleSet's results, with the original DDL */
  val expressionsRuleSetsType: MapType = MapType(packedIdType, expressionsRuleSetType)
  /** The full suite results of expressionRunner's, with the original DDL */
  val expressionsResultsType: StructType = StructType(Seq(StructField("id", packedIdType), StructField("ruleSetResults", expressionsRuleSetsType)))

  /** A given ruleSet's results of packedids to rule results, without the DDL field */
  val expressionsRuleSetNoDDLType: MapType = MapType(packedIdType, StringType)
  /** The collection of expression ruleSet's results, without the DDL field */
  val expressionsRuleSetsNoDDLType: MapType = MapType(packedIdType, expressionsRuleSetNoDDLType)
  /** Unlike expressionsResultsType only the results of the expression cast to string are stored, the resultDDL is not present */
  val expressionsResultsNoDDLType: StructType = StructType(Seq(StructField("id", packedIdType), StructField("ruleSetResults", expressionsRuleSetsNoDDLType)))

}
