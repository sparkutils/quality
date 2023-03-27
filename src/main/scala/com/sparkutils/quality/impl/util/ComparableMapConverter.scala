package com.sparkutils.quality.impl.util

import com.sparkutils.quality.utils.Arrays
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.{InternalRow, util}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, GenericInternalRow, UnaryExpression, UnsafeArrayData}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.types._

object ComparableMapConverter {
  def mapStruct(key: (DataType, Any => Any), value: (DataType, Any => Any), compareF: DataType => Option[(Any, Any) => Int]): (DataType, Any => Any) =
    (ArrayType(StructType(
      Seq(StructField("key", key._1, false),StructField("value", value._1, false))
    ), false),
      (any) => {
        any match {
          case theMap: MapData =>
            // for the key type
            lazy val comparisonOrdering: Ordering[Any] = compareF(key._1).getOrElse(
              sys.error(s"Could not identify the comparison function for type ${key._1} to order keys")
            )(_, _)

            // maps are already converted all the way down before trying to sort
            val sorted = Arrays.toArray(theMap.keyArray(), key._1).zipWithIndex.sortBy(_._1)(comparisonOrdering)
            val vals = Arrays.toArray(theMap.valueArray(), value._1)

            // now re-pack as structs
            ArrayData.toArrayData(sorted.map{
              case (key, index) =>
                InternalRow(key, vals(index))
            })
        }
      }
  )

  def mapStruct(dataType: DataType, compareF: DataType => Option[(Any, Any) => Int]): (DataType, Any => Any) =
    dataType match {
      case mapType: MapType =>
        val key = mapStruct(mapType.keyType, compareF)
        val value = mapStruct(mapType.valueType, compareF)
        mapStruct( key, value, compareF)
      case arrayType: ArrayType =>
        val r = mapStruct(arrayType.elementType, compareF)
        (ArrayType(r._1),
          array =>
            ArrayData.toArrayData( Arrays.mapArray( array.asInstanceOf[util.ArrayData], arrayType.elementType, r._2 ) )
        )
      case structType: StructType =>
        val fieldTransforms = structType.fields.zipWithIndex.map{
          case (f, index) =>
            val p = mapStruct(f.dataType, compareF)
            // use original type to get
            (p._1, (a: InternalRow) => p._2(BoundReference(index, f.dataType, nullable = true).eval(a)))
        }

        // convert types
        (StructType(structType.fields.zip(fieldTransforms).map(p => p._1.copy(dataType = p._2._1))),
          row =>  // convert data to target type
            InternalRow.fromSeq(fieldTransforms.map(_._2(row.asInstanceOf[InternalRow])))
        )
      case _ => (dataType, identity)
    }
}

/**
 * Convert's structures from child replacing all Map's with ordered arrays.
 *
 * Order is provided by registerQualityFunctions compare parameter.
 *
 * @param child the child expression e.g. a quality result
 * @param compareF the compare lookup function
 */
case class ComparableMapConverter(child: Expression, compareF: DataType => Option[(Any, Any) => Int]) extends UnaryExpression with CodegenFallback {

  lazy val (theType, theFunction) = ComparableMapConverter.mapStruct(child.dataType, compareF: DataType => Option[(Any, Any) => Int])

  override def dataType: DataType = theType

  override def nullSafeEval(input: Any): Any = {
    val res = theFunction(input)
    val s = res.asInstanceOf[GenericInternalRow].get(2, ArrayType( StructType( Seq(
          StructField("key",LongType,false),
          StructField("value",StructType(Seq(
            StructField("overallResult",IntegerType,false),
            StructField("ruleResults",ArrayType(StructType(Seq(
              StructField("key",LongType,false),
              StructField("value",IntegerType,false))),false)
              ,true))),false)))
          ,false))
    val s1 = s.asInstanceOf[GenericArrayData].array(0).asInstanceOf[InternalRow].get(1, StructType(Seq(
      StructField("overallResult",IntegerType,false),
      StructField("ruleResults",ArrayType(StructType(Seq(
        StructField("key",LongType,false),
        StructField("value",IntegerType,false))),false)
        ,true)))).asInstanceOf[InternalRow].
      get(1, ArrayType(StructType(Seq(
      StructField("key",LongType,false),
      StructField("value",IntegerType,false))),false))
    val s2 = s1.asInstanceOf[UnsafeArrayData].get(0, StructType(Seq(
      StructField("key",LongType,false),
      StructField("value",IntegerType,false)))).asInstanceOf[InternalRow].get(0, LongType)
    res
  }

  protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)
}
