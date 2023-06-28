package com.sparkutils.quality.impl

import frameless.{Injection, NotCatalystNullable, TypedColumn, TypedEncoder, TypedExpressionEncoder}
import com.sparkutils.quality._
import com.sparkutils.quality.utils.Serializing
import org.apache.spark.sql.{Encoder, Row}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, MapType, StringType, StructField, StructType}
import shapeless.ops.hlist.IsHCons
import shapeless.{HList, LabelledGeneric, Lazy}

import scala.reflect.ClassTag



trait IntEncodersImplicits extends Serializable {

  // RuleResultWithProcessor's are lost in serialization by design, they only make sense in a given run

  implicit val ruleResultToInt: Injection[RuleResult, Int] = Injection(
    {
      case r: RuleResult => Serializing.ruleResultToInt(r)
    },
    {
      case SoftFailedInt => SoftFailed
      case DisabledRuleInt => DisabledRule
      case FailedInt => Failed
      case PassedInt => Passed
      case a: Int => Probability(a.toDouble / PassedInt)
    })

}

object IntEncoders extends IntEncodersImplicits {
}

trait IdEncodersImplicits extends Serializable {
  implicit val versionedIdNotNullable = new NotCatalystNullable[VersionedId] {}

  // try just id first
  implicit val versionedIdTo: Injection[VersionedId, Long] = Injection(
    {
      case Id(id, version) => ((id.toLong) << 32) | (version & 0xffffffffL)
    },
    {
      case a: Long =>
        PackId.unpack(a)
    })
}

object IdEncoders extends IdEncodersImplicits {

}

trait EncodersImplicits extends Serializable {
  import frameless._
  import IntEncoders._
  import IdEncoders._

  implicit val ruleSuiteResultTypedEnc = TypedEncoder[RuleSuiteResult]

  implicit val ruleSuiteResultExpEnc = TypedExpressionEncoder[RuleSuiteResult]

  implicit val ruleSuiteResultDetailsTypedEnc = TypedEncoder[com.sparkutils.quality.RuleSuiteResultDetails]

  implicit val ruleSuiteResultDetailsExpEnc = TypedExpressionEncoder[com.sparkutils.quality.RuleSuiteResultDetails]

  implicit val generalExpressionsResultTypedEnc = TypedEncoder[com.sparkutils.quality.GeneralExpressionsResult]

  implicit val generalExpressionsResultExpEnc = TypedExpressionEncoder[com.sparkutils.quality.GeneralExpressionsResult]

  implicit val generalExpressionResultTypedEnc = TypedEncoder[com.sparkutils.quality.GeneralExpressionResult]

  implicit val generalExpressionResultExpEnc = TypedExpressionEncoder[com.sparkutils.quality.GeneralExpressionResult]

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

}

object Encoders extends EncodersImplicits {

}

object EmptyMap extends MapData {
  override val keyArray: ArrayData = new org.apache.spark.sql.catalyst.util.GenericArrayData(Array.empty)
  override val valueArray: ArrayData = new org.apache.spark.sql.catalyst.util.GenericArrayData(Array.empty)
  override def numElements(): Int = 0

  override def copy(): MapData = this

  override def toString: String = {
    s"keys: $keyArray, values: $valueArray"
  }
}

object MapUtils {
  def toScalaMap(map: MapData, keyType: DataType, valueType: DataType): Map[Any, Any] = {
    val keys = map.keyArray.toObjectArray(keyType)
    val values = map.valueArray.toObjectArray(valueType)
    keys.zip(values).toMap
  }

  /**
   * Assuming MapData entries are stable, as they are array backed, over a dataset.  When the position of 'what' is not in the cachedPositions a full search is performed with the new location being returned in addition.
   *
   * @param mapData the source mapdata to search through.
   * @param cachedPositions already known positions to check for what
   * @param what compared against the keyArray to obtain the position
   * @param getValue obtain the value from the mapData, with any necessary conversions, against the index parameter
   * @tparam T
   * @return (result from getValue or null when not found, either cachedPositions or cachedPositions and a newly found position)
   */
  def getMapEntry[T <: AnyRef](mapData: MapData, cachedPositions: Seq[Int], what: Any)(getValue: Int => T): (T, Seq[Int]) = {
    val keys = mapData.keyArray()
    val n = keys.numElements()
    val acc = (idx: Int) => keys.getLong(idx)

    def search() = {
      var i = 0
      var found = false
      while (i < n && !found) {
        if (acc(i) == what) {
          found = true
        } else {
          i += 1
        }
      }
      if (!found)
        -1
      else
        i
    }

    def withCached() =
      cachedPositions.find { i =>
        if (i < n)
          acc(i) == what
        else
          false
      }

    withCached().map(i => (getValue(i), cachedPositions))
      .getOrElse {
        val r = search()
        if (r == -1)
          (null.asInstanceOf[T], cachedPositions)
        else
          (getValue(r), cachedPositions :+ r)
      }
  }

}
