package com.sparkutils.quality.impl

import frameless.{Injection, NotCatalystNullable}
import com.sparkutils.quality._
import com.sparkutils.quality.impl.util.Serializing
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.DataType

import scala.reflect.ClassTag

trait IntEncodersImplicits extends Serializable {

  // RuleResultWithProcessor's are lost in serialization by design, they only make sense in a given run

  /**
   * Converts the DQ results to and from Int and RuleResult's.  Probability is processed by the value / PassedInt
   */
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
