package com.sparkutils.quality.impl

import com.sparkutils.quality.QualityException.qualityException
import com.sparkutils.quality.impl.RuleRegistrationFunctions.getBinary
import com.sparkutils.quality.impl.RuleSuiteHelpers.deserialize
import com.sparkutils.quality.impl.util.CombinedRuleSuiteRows
import com.sparkutils.quality.{GeneralExpressionsResult, RuleEngineResult, RuleFolderResult, RuleSuite, RuleSuiteResult, rule_suite}
import frameless.TypedEncoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{AgnosticEncoder, ExpressionEncoder}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, VariableReference}
import org.apache.spark.sql.functions.{lit, named_struct, struct}
import org.apache.spark.sql.{Column, Encoder, Row, ShimUtils, SparkSession}
import org.apache.spark.sql.types.{BinaryType, DataType, StructType}
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