package com.sparkutils.quality.impl.util

import com.sparkutils.quality.utils.Arrays
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, MapData}
import org.apache.spark.sql.catalyst.{InternalRow, util}
import org.apache.spark.sql.types._

/**
 * Convert maps to sorted arrays of key value structs to allow comparison
 */
object ComparableMapConverter {

  def deMapStruct(key: (DataType, Any => Any), value: (DataType, Any => Any), compareF: DataType => Option[(Any, Any) => Int]): (DataType, Any => Any) =
    (ArrayType(StructType(
      Seq(StructField("key", key._1, false), StructField("value", value._1, false))
    ), false),
      {
        case theMap: MapData =>
          // for the key type
          lazy val comparisonOrdering: Ordering[Any] = compareF(key._1).getOrElse(
            sys.error(s"Could not identify the comparison function for type ${key._1} to order keys")
          )(_, _)

          // maps are already converted all the way down before trying to sort
          val sorted = Arrays.toArray(theMap.keyArray(), key._1).zipWithIndex.sortBy(_._1)(comparisonOrdering)
          val vals = Arrays.toArray(theMap.valueArray(), value._1)

          // now re-pack as structs
          ArrayData.toArrayData(sorted.map {
            case (tkey, index) =>
              InternalRow(key._2(tkey), value._2(vals(index)))
          })
      }
  )

  def deMapStruct(dataType: DataType, compareF: DataType => Option[(Any, Any) => Int]): (DataType, Any => Any) =
    dataType match {
      case mapType: MapType =>
        val key = deMapStruct(mapType.keyType, compareF)
        val value = deMapStruct(mapType.valueType, compareF)
        deMapStruct( key, value, compareF)
      case arrayType: ArrayType =>
        val r = deMapStruct(arrayType.elementType, compareF)
        (ArrayType(r._1),
          {
            case array: util.ArrayData =>
              ArrayData.toArrayData(
                Arrays.mapArray( array, arrayType.elementType, r._2 )
              )
          }
        )
      case structType: StructType =>
        val fieldTransforms = structType.fields.zipWithIndex.map{
          case (f, index) =>
            val p = deMapStruct(f.dataType, compareF)
            // use original type to get
            (p._1, (a: InternalRow) => p._2(BoundReference(index, f.dataType, nullable = true).eval(a)))
        }

        // convert types
        (StructType(structType.fields.zip(fieldTransforms).map(p => p._1.copy(dataType = p._2._1))),
          { // convert data to target type
            case row: InternalRow =>
              InternalRow.fromSeq(fieldTransforms.map(_._2(row)))
          }
        )
      case _ => (dataType, identity)
    }

  object KeyValueArray {
    def unapply(dataType: DataType): Option[(DataType, DataType)] =
      dataType match {
        case arrayType: ArrayType =>
          arrayType.elementType match {
            case s : StructType if s.fields.size == 2 && s.fields(0).name == "key" && s.fields(1).name == "value" =>
              Some((s.fields(0).dataType, s.fields(1).dataType))
            case _ => None
          }
        case _ => None
      }
  }

  def mapStruct(key: (DataType, Any => Any), value: (DataType, Any => Any)): (DataType, Any => Any) =
    (MapType(key._1, value._1, false),
      {
        case theArray: ArrayData =>

          val keyr = BoundReference(0, key._1, nullable = true)
          val valuer = BoundReference(1, value._1, nullable = true)

          val theScalaMap =
            Arrays.mapArray(theArray, StructType(
              Seq(StructField("key", key._1, false), StructField("value", value._1, false))
            ), {
              case row: InternalRow =>
                (key._2(keyr.eval(row)), value._2(valuer.eval(row)))
            } ).map(_.asInstanceOf[(Any, Any)]).toMap

          ArrayBasedMapData(theScalaMap)
      }
    )

  def mapStruct(dataType: DataType): (DataType, Any => Any) =
    dataType match {
      case KeyValueArray(keyType, valueType) =>
        val key = mapStruct(keyType)
        val value = mapStruct(valueType)
        mapStruct( key, value)
      case arrayType: ArrayType =>
        val r = mapStruct(arrayType.elementType)
        (ArrayType(r._1),
          {
            case array: util.ArrayData =>
              ArrayData.toArrayData(
                Arrays.mapArray( array, arrayType.elementType, r._2 )
              )
          }
        )
      case structType: StructType =>
        val fieldTransforms = structType.fields.zipWithIndex.map{
          case (f, index) =>
            val p = mapStruct(f.dataType)
            // use original type to get
            (p._1, (a: InternalRow) => p._2(BoundReference(index, f.dataType, nullable = true).eval(a)))
        }

        // convert types
        (StructType(structType.fields.zip(fieldTransforms).map(p => p._1.copy(dataType = p._2._1))),
          { // convert data to target type
            case row: InternalRow =>
              InternalRow.fromSeq(fieldTransforms.map(_._2(row)))
          }
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

  lazy val (theType, theFunction) = ComparableMapConverter.deMapStruct(child.dataType, compareF: DataType => Option[(Any, Any) => Int])

  override def dataType: DataType = theType

  override def nullSafeEval(input: Any): Any = theFunction(input)

  protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)
}

/**
 * Reverts the ComparableMapConverter
 *
 * @param child the child expression e.g. a quality result
 */
case class ComparableMapReverser(child: Expression) extends UnaryExpression with CodegenFallback {

  lazy val (theType, theFunction) = ComparableMapConverter.mapStruct(child.dataType)

  override def dataType: DataType = theType

  override def nullSafeEval(input: Any): Any = theFunction(input)

  protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)
}
