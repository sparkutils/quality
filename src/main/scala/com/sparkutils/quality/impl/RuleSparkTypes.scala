package com.sparkutils.quality.impl

import com.sparkutils.quality.types.expressionResultTypeYaml
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
  val expressionResultTypeYaml: StructType = StructType(Seq(StructField("result", StringType), StructField("resultDDL", StringType)))
  /** A given ruleSet's results of packedId's expressionResultType, with the original DDL */
  def expressionsRuleSetType(endType: DataType): MapType = MapType(packedIdType, endType)
  /** The collection of expression ruleSet's results, with the original DDL */
  def expressionsRuleSetsType(endType: DataType): MapType = MapType(packedIdType, expressionsRuleSetType(endType))
  /** The full suite results of expressionRunner's, with the original DDL */
  def expressionsResultsType(endType: DataType): StructType = StructType(Seq(StructField("id", packedIdType), StructField("ruleSetResults", expressionsRuleSetsType(endType))))

  object ExpressionsResultsType {
    def unapply(arg: DataType): Option[DataType] = arg match {
      case StructType(Array(id: StructField,rr: StructField))
        if id.name == "id" && id.dataType == packedIdType =>
        rr.dataType match {
          case m: MapType if m.keyType == packedIdType =>
            m.valueType match {
              case m: MapType if m.keyType == packedIdType =>
                m.valueType match {
                  case StructType(Array(res, _)) if res.dataType == StringType && res.name == "result" => Some(expressionResultTypeYaml)
                  case dt: DataType => Some(dt)
                }
            }
          case _ => None
        }
      case _ => None
    }

  }

  /** A given ruleSet's results of packedids to rule results, without the DDL field */
  val expressionsRuleSetNoDDLType: MapType = MapType(packedIdType, StringType)
  /** The collection of expression ruleSet's results, without the DDL field */
  val expressionsRuleSetsNoDDLType: MapType = MapType(packedIdType, expressionsRuleSetNoDDLType)
  /** Unlike expressionsResultsType only the results of the expression cast to string are stored, the resultDDL is not present */
  val expressionsResultsNoDDLType: StructType = StructType(Seq(StructField("id", packedIdType), StructField("ruleSetResults", expressionsRuleSetsNoDDLType)))

}
