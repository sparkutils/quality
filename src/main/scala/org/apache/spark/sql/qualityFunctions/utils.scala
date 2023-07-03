package org.apache.spark.sql.qualityFunctions

import com.sparkutils.quality.impl.util.Comparison.compareToOrdering
import org.apache.spark.sql.{Column, QualitySparkUtils}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, LambdaFunction, NamedExpression, UnresolvedNamedLambdaVariable}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, AtomicType, DataType, MapType, StructField, StructType}

object utils {

  def named(col: Column): NamedExpression = col.named

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
      case dt: AtomicType => Some(compareToOrdering(QualitySparkUtils.sparkOrdering(dt)))
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

  // taken from functions, where they are private
  def createLambda(f: Column => Column) = {
    val x = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariable.freshVarName("x")))
    val function = f(Column(x)).expr
    LambdaFunction(function, Seq(x))
  }

  def createLambda(f: (Column, Column) => Column) = {
    val x = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariable.freshVarName("x")))
    val y = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariable.freshVarName("y")))
    val function = f(Column(x), Column(y)).expr
    LambdaFunction(function, Seq(x, y))
  }

  def createLambda(f: (Column, Column, Column) => Column) = {
    val x = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariable.freshVarName("x")))
    val y = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariable.freshVarName("y")))
    val z = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariable.freshVarName("z")))
    val function = f(Column(x), Column(y), Column(z)).expr
    LambdaFunction(function, Seq(x, y, z))
  }

}
