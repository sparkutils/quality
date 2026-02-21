package com.sparkutils.quality.impl

import com.sparkutils.quality.{CombinedRuleRow, CombinedRuleSuiteRows}
import com.sparkutils.quality.{GeneralExpressionResult, GeneralExpressionsResult, GeneralExpressionsResultNoDDL, RuleEngineResult, RuleFolderResult, RuleResult, RuleSetResult, RuleSetStatistics, RuleStatistics, RuleSuiteGroupStatistics, RuleSuiteResult, RuleSuiteResultDetails, RuleSuiteStatistics, VersionedId}
import frameless.TypedEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Encoder, Row, ShimUtils, SparkSession}
import shapeless.{HList, LabelledGeneric, Lazy}
import shapeless.ops.hlist.IsHCons

import scala.reflect.ClassTag

trait EncodersImplicits extends Serializable {
  import frameless._
  import IntEncoders._
  import IdEncoders._

  implicit val ruleSuiteResultTypedEnc = TypedEncoder[RuleSuiteResult]

  implicit val ruleSuiteResultExpEnc: Encoder[RuleSuiteResult] = TypedExpressionEncoder[RuleSuiteResult]

  implicit val ruleSuiteResultDetailsTypedEnc = TypedEncoder[com.sparkutils.quality.RuleSuiteResultDetails]

  implicit val ruleSuiteResultDetailsExpEnc: Encoder[RuleSuiteResultDetails] = TypedExpressionEncoder[com.sparkutils.quality.RuleSuiteResultDetails]

  implicit def generalExpressionsResultTypedEnc[R: TypedEncoder] = TypedEncoder[com.sparkutils.quality.GeneralExpressionsResult[R]]

  implicit def generalExpressionsResultExpEnc[R](implicit ev: TypedEncoder[GeneralExpressionsResult[R]]): Encoder[GeneralExpressionsResult[R]] = TypedExpressionEncoder[com.sparkutils.quality.GeneralExpressionsResult[R]]

  implicit val generalExpressionResultTypedEnc = TypedEncoder[com.sparkutils.quality.GeneralExpressionResult]

  implicit val generalExpressionResultExpEnc: Encoder[GeneralExpressionResult] = TypedExpressionEncoder[com.sparkutils.quality.GeneralExpressionResult]

  implicit val generalExpressionsResultNoDDLTypedEnc = TypedEncoder[com.sparkutils.quality.GeneralExpressionsResultNoDDL]

  implicit val generalExpressionsResultNoDDLExpEnc: Encoder[GeneralExpressionsResultNoDDL] = TypedExpressionEncoder[com.sparkutils.quality.GeneralExpressionsResultNoDDL]

  implicit def ruleEngineResultTypedEnc[T: TypedEncoder, G <: HList, H <: HList](implicit
                                                                                 i0: LabelledGeneric.Aux[RuleEngineResult[T], G],
                                                                                 i1: DropUnitValues.Aux[G, H],
                                                                                 i2: IsHCons[H],
                                                                                 i3: Lazy[RecordEncoderFields[H]],
                                                                                 i5: ClassTag[RuleEngineResult[T]]
                                                                                ): TypedEncoder[RuleEngineResult[T]] = {
    TypedEncoder.usingDerivation[RuleEngineResult[T], G, H]
  }

  implicit def ruleEngineResultExpEnc[T: TypedEncoder]: Encoder[RuleEngineResult[T]] = TypedExpressionEncoder[RuleEngineResult[T]]

  implicit def ruleFolderResultTypedEnc[T: TypedEncoder, G <: HList, H <: HList](implicit
                                                                                 i0: LabelledGeneric.Aux[RuleFolderResult[T], G],
                                                                                 i1: DropUnitValues.Aux[G, H],
                                                                                 i2: IsHCons[H],
                                                                                 i3: Lazy[RecordEncoderFields[H]],
                                                                                 i5: ClassTag[RuleFolderResult[T]]
                                                                                ): TypedEncoder[RuleFolderResult[T]] = {
    TypedEncoder.usingDerivation[RuleFolderResult[T], G, H]
  }

  implicit def ruleFolderResultExpEnc[T: TypedEncoder]: Encoder[RuleFolderResult[T]] = TypedExpressionEncoder[RuleFolderResult[T]]

  implicit val ruleStatisticsTypedEnc = TypedEncoder[RuleStatistics]

  implicit val ruleStatisticsTypedExpEnc: Encoder[RuleStatistics]  = TypedExpressionEncoder[RuleStatistics]

  implicit val ruleSetStatisticsTypedEnc = TypedEncoder[RuleSetStatistics]

  implicit val ruleSetStatisticsTypedExpEnc: Encoder[RuleSetStatistics]  = TypedExpressionEncoder[RuleSetStatistics]

  implicit val ruleSuiteStatisticsTypedEnc = TypedEncoder[RuleSuiteStatistics]

  implicit val ruleSuiteStatisticsTypedExpEnc: Encoder[RuleSuiteStatistics]  = TypedExpressionEncoder[RuleSuiteStatistics]

  implicit val ruleSuiteGroupStatisticsTypedEnc = TypedEncoder[RuleSuiteGroupStatistics]

  implicit val ruleSuiteGroupStatisticsTypedExpEnc: Encoder[RuleSuiteGroupStatistics]  = TypedExpressionEncoder[RuleSuiteGroupStatistics]

  implicit val combinedRuleRowTypedEnc = TypedEncoder[CombinedRuleRow]

  implicit val combinedRuleRowTypedExpEnc = TypedExpressionEncoder[CombinedRuleRow]

  implicit val combinedRuleSuiteRowTypedEnc = TypedEncoder[CombinedRuleSuiteRows]

  implicit val combinedRuleSuiteRowTypedExpEnc = TypedExpressionEncoder[CombinedRuleSuiteRows]

  implicit val combinedSeqRuleSuiteRowTypedEnc = TypedEncoder[Seq[CombinedRuleSuiteRows]]

  implicit val combinedSeqRuleSuiteRowTypedExpEnc = TypedExpressionEncoder[Seq[CombinedRuleSuiteRows]]

}

object VariableHelper {
  /**
   * creates a variable.  The expr is set with 'set var' allowing queries, the default is null.
   *
   * NB declare or replace is used.
   *
   * @param stableName
   * @param ddl
   */
  def createVar(stableName: String, ddl: String, expr: String): Unit = {
    // Defaults can't have subqueries
    val defaultCommand = s"declare or replace variable `$stableName` $ddl default null;"
    SparkSession.active.sql(defaultCommand)
    val setCommand = s"set var `$stableName` = $expr;"
    SparkSession.active.sql(setCommand)
  }
}

object Encoders extends EncodersImplicits {

  def rowTypedEnc(rowType: DataType): TypedEncoder[Row] =
    new TypedEncoder[Row]()(ClassTag(classOf[Row])) {
      override def nullable: Boolean = agnosticEncoder.nullable

      override val agnosticEncoder: AgnosticEncoder[Row] = ShimUtils.rowEncoder(rowType.asInstanceOf[StructType])
    }

}
