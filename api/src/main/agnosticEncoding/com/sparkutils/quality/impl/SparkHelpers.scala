package com.sparkutils.quality.impl

import com.sparkutils.quality.{GeneralExpressionResult, GeneralExpressionsResult, GeneralExpressionsResultNoDDL, RuleEngineResult, RuleFolderResult, RuleSuite, RuleSuiteResult, RuleSuiteResultDetails, rule_suite}
import org.apache.spark.sql.{Encoder, SparkSession}
import shapeless.{HList, LabelledGeneric, Lazy}
import shapeless.ops.hlist.IsHCons

import scala.reflect.ClassTag

trait EncodersImplicits extends Serializable {
  import frameless._
  import IntEncoders._
  import IdEncoders._

  implicit val ruleSuiteResultTypedEnc: TypedEncoder[RuleSuiteResult] = TypedEncoder[RuleSuiteResult]

  implicit val ruleSuiteResultExpEnc: Encoder[RuleSuiteResult] = TypedExpressionEncoder[RuleSuiteResult]

  implicit val ruleSuiteResultDetailsTypedEnc: TypedEncoder[RuleSuiteResultDetails] = TypedEncoder[com.sparkutils.quality.RuleSuiteResultDetails]

  implicit val ruleSuiteResultDetailsExpEnc: Encoder[RuleSuiteResultDetails] = TypedExpressionEncoder[com.sparkutils.quality.RuleSuiteResultDetails]

  implicit def generalExpressionsResultTypedEnc[R: TypedEncoder]: TypedEncoder[GeneralExpressionsResult[R]] = TypedEncoder[com.sparkutils.quality.GeneralExpressionsResult[R]]

  implicit def generalExpressionsResultExpEnc[R](implicit ev: TypedEncoder[GeneralExpressionsResult[R]]): Encoder[GeneralExpressionsResult[R]] = TypedExpressionEncoder[com.sparkutils.quality.GeneralExpressionsResult[R]]

  implicit val generalExpressionResultTypedEnc: TypedEncoder[GeneralExpressionResult] = TypedEncoder[com.sparkutils.quality.GeneralExpressionResult]

  implicit val generalExpressionResultExpEnc: Encoder[GeneralExpressionResult] = TypedExpressionEncoder[com.sparkutils.quality.GeneralExpressionResult]

  implicit val generalExpressionsResultNoDDLTypedEnc: TypedEncoder[GeneralExpressionsResultNoDDL] = TypedEncoder[com.sparkutils.quality.GeneralExpressionsResultNoDDL]

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