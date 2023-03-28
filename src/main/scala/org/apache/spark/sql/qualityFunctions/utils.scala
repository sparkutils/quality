package org.apache.spark.sql.qualityFunctions

import com.sparkutils.quality.utils.Comparison.compareToOrdering
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.types.{ArrayType, AtomicType, DataType, MapType, StructType}

object utils {

  private val noopCompare = (dt: DataType) => None

  def defaultMapCompare(dataType: DataType, extension: DataType => Option[(Any, Any) => Int] = noopCompare): Option[(Any, Any) => Int] =
    dataType match {
      case mt: MapType => {
        val kt = defaultMapCompare(mt.keyType, extension).
          getOrElse(sys.error(s"Could not find compare function for map key type ${mt.keyType}"))
        val vt = defaultMapCompare(mt.valueType, extension).
          getOrElse(sys.error(s"Could not find compare function for map value type ${mt.valueType}"))

        val kr = BoundReference(0, mt.keyType, nullable = true)
        val vr = BoundReference(1, mt.valueType, nullable = true)

        // actual field is in a key val struct
        Some((left, right) => {
          val lkv = kr.eval(left.asInstanceOf[InternalRow])
          val rkv = kr.eval(right.asInstanceOf[InternalRow])
          val lvv = vr.eval(left.asInstanceOf[InternalRow])
          val rvv = vr.eval(right.asInstanceOf[InternalRow])

          val r = kt(lkv, rkv)
          if (r != 0)
            r
          else
            vt(lvv, rvv)
        })
      }
      case dt: AtomicType => Some(compareToOrdering(dt.ordering))
      case arrayType: ArrayType => defaultMapCompare(arrayType.elementType, extension)
      case structType: StructType =>
        val allFields = structType.fields.zipWithIndex.map{
          case (f, index) =>
            defaultMapCompare( f.dataType, extension).map(cf => (BoundReference(index, f.dataType, nullable = true), cf)).
              getOrElse(sys.error(s"Could not find compare function for ${f.dataType} in struct $structType"))
        }
        Some(
          // fail early for any non 0
          (left, right) => {
            var i = 0
            var res = 0
            while (i < allFields.size && res == 0) {
              val leftV = allFields(i)._1.eval(left.asInstanceOf[InternalRow])
              val rightV = allFields(i)._1.eval(right.asInstanceOf[InternalRow])
              res = allFields(i)._2(leftV, rightV)
              i += 1
            }
            res
          }
        )
      case _ => extension(dataType).orElse(
        sys.error(s"Could not find compare function for ${dataType}")
      )
    }

}
