package org.apache.spark.sql.qualityFunctions

import com.sparkutils.quality.impl.RuleSuiteHelpers.getContextOrSparkClassLoader
import com.sparkutils.quality.impl.util.Comparison.compareToOrdering
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.{ShimUtils, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream, ObjectStreamClass}
import scala.reflect.ClassTag

object utils {

  private val noopCompare = (dt: DataType) => None

  object KeyValueArray {
    def unapply(dataType: DataType): Option[(DataType, DataType)] =
      dataType match {
        case arrayType: ArrayType =>
          arrayType.elementType match {
            case s: StructType if s.fields.size == 2 && s.fields(0).name == "key" && s.fields(1).name == "value" =>
              Some((s.fields(0).dataType, s.fields(1).dataType))
            case _ => None
          }
        case _ => None
      }
  }

  def keyValueType(keyType: DataType, valueType: DataType) =
    ArrayType(StructType(
      Seq(StructField("key", keyType, false), StructField("value", valueType, false))
    ), false)

  def genArrayCompare(elementType: DataType, elComp: (Any, Any) => Int): (Any, Any) => Int = (oleft, oright) => {
    val left = oleft.asInstanceOf[ArrayData]
    val right = oright.asInstanceOf[ArrayData]
    if (left.numElements() > right.numElements())
      1
    else if (left.numElements() < right.numElements())
      -1
    else {
      // same
      var i = 0
      var res = 0
      val al = (idx: Int) => left.get(idx, elementType)// Arrays.toArray(left, elementType)
      val ar = (idx: Int) => right.get(idx, elementType)// Arrays.toArray(right, elementType)
      while (i < left.numElements() && res == 0) {
        res = elComp(al(i), ar(i))
        i += 1
      }
      res
    }
  }

  def defaultMapCompare(dataType: DataType, extension: DataType => Option[(Any, Any) => Int] = noopCompare): Option[(Any, Any) => Int] =
    dataType match {
      case mt: MapType => {
        val kt = defaultMapCompare(mt.keyType, extension).
          getOrElse(sys.error(s"Could not find compare function for map key type ${mt.keyType}"))

        val kr = BoundReference(0, mt.keyType, nullable = true)

        // actual field is in a key val struct
        Some(
          genArrayCompare(keyValueType(mt.keyType, mt.valueType),
            (left, right) => {
              val lkv = kr.eval(left.asInstanceOf[InternalRow])
              val rkv = kr.eval(right.asInstanceOf[InternalRow])

              val r = kt(lkv, rkv)
              if (r != 0)
                r
              else {
                /* shouldn't happen unless MapData is created directly and it's not
                   actually a map checking on key uniqueness, here for completeness */
                val vt = defaultMapCompare(mt.valueType, extension).
                  getOrElse(sys.error(s"Could not find compare function for map value type ${mt.valueType}"))
                val vr = BoundReference(1, mt.valueType, nullable = true)

                val lvv = vr.eval(left.asInstanceOf[InternalRow])
                val rvv = vr.eval(right.asInstanceOf[InternalRow])

                vt(lvv, rvv)
              }
            })
        )
      }
      case dt: AtomicType => Some(compareToOrdering(ShimUtils.sparkOrdering(dt)))
      case arrayType: ArrayType =>
        val oelcomp = defaultMapCompare(arrayType.elementType, extension)
        oelcomp.map{
          elcomp =>
            genArrayCompare(arrayType.elementType, elcomp)
        }
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

  // unlike serializeImpl this compresses using sparks compression which will work on executors
  def compress(obj: Object with Serializable): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val cos = CompressionCodec.createCodec(SparkContext.getActive.get.conf).compressedOutputStream(bos)
    val os = new ObjectOutputStream(cos)
    // get rid of List's Vectors are serializable
    os.writeObject(obj)
    val res = bos.toByteArray
    os.close()
    res
  }

  def decompress[T: ClassTag](arr: Array[Byte]) = {
    val bis = new ByteArrayInputStream(arr)
    val cis = CompressionCodec.createCodec(SparkContext.getActive.get.conf).compressedInputStream(bis)
    val os = new ObjectInputStream(cis) {
      override def resolveClass(desc: ObjectStreamClass): Class[_] =
        Class.forName(desc.getName, false, getContextOrSparkClassLoader)
    }
    val suite = os.readObject()
    os.close()
    suite.asInstanceOf[T]
  }
}

