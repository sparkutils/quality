package com.sparkutils.quality.impl

import com.sparkutils.quality.{GeneralExpressionsResult, RuleEngineResult, RuleFolderResult, RuleSetStatistics, RuleStatistics, RuleSuiteGroupResults, RuleSuiteGroupStatistics, RuleSuiteResult, RuleSuiteStatistics}
import org.apache.spark.sql.Encoder
import shapeless.{HList, LabelledGeneric, Lazy}
import shapeless.ops.hlist.IsHCons

import scala.reflect.ClassTag

trait EncodersImplicits extends Serializable {
  import frameless._
  import IntEncoders._
  import IdEncoders._

  implicit val ruleSuiteResultTypedEnc = TypedEncoder[RuleSuiteResult]

  implicit val ruleSuiteResultExpEnc = TypedExpressionEncoder[RuleSuiteResult]

  implicit val ruleSuiteResultDetailsTypedEnc = TypedEncoder[com.sparkutils.quality.RuleSuiteResultDetails]

  implicit val ruleSuiteResultDetailsExpEnc = TypedExpressionEncoder[com.sparkutils.quality.RuleSuiteResultDetails]

  implicit def generalExpressionsResultTypedEnc[R: TypedEncoder] = TypedEncoder[com.sparkutils.quality.GeneralExpressionsResult[R]]

  implicit def generalExpressionsResultExpEnc[R](implicit ev: TypedEncoder[GeneralExpressionsResult[R]]) = TypedExpressionEncoder[com.sparkutils.quality.GeneralExpressionsResult[R]]

  implicit val generalExpressionResultTypedEnc = TypedEncoder[com.sparkutils.quality.GeneralExpressionResult]

  implicit val generalExpressionResultExpEnc = TypedExpressionEncoder[com.sparkutils.quality.GeneralExpressionResult]

  implicit val generalExpressionsResultNoDDLTypedEnc = TypedEncoder[com.sparkutils.quality.GeneralExpressionsResultNoDDL]

  implicit val generalExpressionsResultNoDDLExpEnc = TypedExpressionEncoder[com.sparkutils.quality.GeneralExpressionsResultNoDDL]

  implicit def ruleEngineResultTypedEnc[T: TypedEncoder, G <: HList, H <: HList](implicit
                                                                                 i0: LabelledGeneric.Aux[RuleEngineResult[T], G],
                                                                                 i1: DropUnitValues.Aux[G, H],
                                                                                 i2: IsHCons[H],
                                                                                 i3: Lazy[RecordEncoderFields[H]],
                                                                                 i4: Lazy[NewInstanceExprs[G]],
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
                                                                                 i4: Lazy[NewInstanceExprs[G]],
                                                                                 i5: ClassTag[RuleFolderResult[T]]
                                                                                ): TypedEncoder[RuleFolderResult[T]] = {
    TypedEncoder.usingDerivation[RuleFolderResult[T], G, H]
  }

  implicit def ruleFolderResultExpEnc[T: TypedEncoder]: Encoder[RuleFolderResult[T]] = TypedExpressionEncoder[RuleFolderResult[T]]

  implicit val ruleStatisticsTypedEnc = TypedEncoder[RuleStatistics]

  implicit val ruleStatisticsTypedExpEnc = TypedExpressionEncoder[RuleStatistics]

  implicit val ruleSetStatisticsTypedEnc = TypedEncoder[RuleSetStatistics]

  implicit val ruleSetStatisticsTypedExpEnc = TypedExpressionEncoder[RuleSetStatistics]

  implicit val ruleSuiteStatisticsTypedEnc = TypedEncoder[RuleSuiteStatistics]

  implicit val ruleSuiteStatisticsTypedExpEnc = TypedExpressionEncoder[RuleSuiteStatistics]

  implicit val ruleSuiteGroupStatisticsTypedEnc = TypedEncoder[RuleSuiteGroupStatistics]

  implicit val ruleSuiteGroupStatisticsTypedExpEnc = TypedExpressionEncoder[RuleSuiteGroupStatistics]

  implicit val ruleSuiteGroupResultsTypedEnc = TypedEncoder[RuleSuiteGroupResults]

  implicit val ruleSuiteGroupResultsTypedExpEnc = TypedExpressionEncoder[RuleSuiteGroupResults]

}
