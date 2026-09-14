package com.sparkutils.quality.impl.util

import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.types.{DataType, IntegerType, LongType}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


object Arrays {
  /**
   * UnsafeArrayData doesn't allow calling .array, foreach when needed and for others use array
   *
   * @param array
   * @param dataType
   * @param f
   * @return
   */
  def mapArray[T: ClassTag](array: ArrayData, dataType: DataType, f: Any => T, by: Int = 0): Array[T] =
    array match {
      case _: UnsafeArrayData =>
        val res = Array.ofDim[T](array.numElements() + by)
        array.foreach(dataType, (i, v) => res.update(i, f(v)))
        res
      case _ => {
        val r = array.array.map(f)
        if (by == 0)
          r
        else {
          val res = Array.ofDim[T](array.numElements() + by)
          r.copyToArray(res)
          res
        }

      }
    }

  /**
   * gets an array out of UnsafeArrayData or others
   * @param array
   * @param dataType
   * @return
   */
  def toArray(array: ArrayData, dataType: DataType): Array[Any] =
    array match {
      case _: UnsafeArrayData =>
        mapArray(array, dataType, identity)
      case _ => array.array
    }

}

class IntegerArray(val ints: Array[Int]) extends GenericArrayData(null: Array[Any]) {

  override def numElements(): Int = ints.length

  override def copy(): IntegerArray = {
    new IntegerArray(ints.clone())
  }

  override def isNullAt(ordinal: Int): Boolean = false

  def update(i: Int, value: Int): Unit = ints.update(i, value)

  override def getInt(ordinal: Int): Int = ints(ordinal)

  override def get(ordinal: Int, dataType: DataType): AnyRef =
    if (dataType == IntegerType)
      getInt(ordinal).asInstanceOf[Integer]
    else
      ???

}

class LongArray(val longs: Array[Long]) extends GenericArrayData(null: Array[Any]) {

  override def numElements(): Int = longs.length

  override def copy(): LongArray = {
    new LongArray(longs.clone())
  }

  override def isNullAt(ordinal: Int): Boolean = false

  override def getLong(ordinal: Int): Long = longs(ordinal)

  override def get(ordinal: Int, dataType: DataType): AnyRef =
    if (dataType == LongType)
      getLong(ordinal).asInstanceOf[java.lang.Long]
    else
      ???

}

class RuleSetMap(val ids: LongArray, val results: IntegerArray) extends MapData {
  override val keyArray: ArrayData = ids
  override val valueArray: ArrayData = results
  override def numElements(): Int = 0

  override def copy(): MapData = {
    new RuleSetMap(ids.copy(), results.copy())
  }

  override def toString: String = {
    s"keys: $keyArray, values: $valueArray"
  }
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
    val keys = map.keyArray().toObjectArray(keyType)
    val values = map.valueArray().toObjectArray(valueType)
    keys.zip(values).toMap
  }

  def toScalaMapKeysConverted(map: MapData, keyType: DataType, valueType: DataType): Map[Any, Any] = {
    val keyConverter = CatalystTypeConverters.createToScalaConverter(keyType)
    val keys = map.keyArray().toObjectArray(keyType).map(keyConverter)
    val values = map.valueArray().toObjectArray(valueType)
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
  def getMapEntry[T <: AnyRef](mapData: MapData, cachedPositions: ArrayBuffer[Int], keyTest: (ArrayData, Int) => Boolean )(getValue: Int => T): (T, ArrayBuffer[Int]) = {
    val keys = mapData.keyArray()
    val n = keys.numElements()

    def withCached() =
      cachedPositions.find { i =>
        if (i < n)
          keyTest(keys, i)
        else
          false
      }

    withCached().map(i => (getValue(i), cachedPositions))
      .getOrElse {
        val r = getKeyIndex(mapData, keyTest)
        if (r == -1)
          (null.asInstanceOf[T], cachedPositions)
        else
          (getValue(r), cachedPositions :+ r)
      }
  }

  /**
   * Grows a map by a given number of elements
   * @param map
   * @param kt
   * @param vt
   * @return
   */
  def growMap(map: MapData, kt: DataType, vt: DataType, withPairs: (Any, Any) *): MapData = {
    val ns = map.numElements() + withPairs.length
    val nka = Arrays.mapArray(map.keyArray(), kt, identity, withPairs.length)
    for(i <- (map.numElements() until ns).zipWithIndex) {
      nka.update(i._1, withPairs(i._2)._1)
    }
    val nk = new GenericArrayData(nka)

    val nva = Arrays.mapArray(map.valueArray(), vt, identity, withPairs.length)
    for(i <- (map.numElements() until ns).zipWithIndex) {
      nva.update(i._1, withPairs(i._2)._2)
    }
    val nv = new GenericArrayData(nva)
    new ArrayBasedMapData(nk, nv)
  }

  /**
   * Replaces a non-primitive map entry, which requires full copy
   * @param map
   * @param kt
   * @param vt
   * @param i
   * @param withPair
   * @return
   */
  def replaceEntry(map: MapData, kt: DataType, vt: DataType, i: Int, withPair: (Any, Any)): MapData =
    (map.keyArray(), map.valueArray()) match {
      case (k: GenericArrayData, v: GenericArrayData) =>
        // change in place
        k.array.update(i, withPair._1)
        v.array.update(i, withPair._2)
        map
      case _ =>
        val nka = Arrays.mapArray(map.keyArray(), kt, identity)
        nka.update(i, withPair._1)
        val nk = new GenericArrayData(nka)

        val nva = Arrays.mapArray(map.valueArray(), vt, identity)
        nva.update(i, withPair._2)

        val nv = new GenericArrayData(nva)
        new ArrayBasedMapData(nk, nv)
    }

  /**
   * finds matching entries in the keyArray
   * @param map
   * @param equal
   * @return -1 when not found, otherwise the index of the matching kez
   */
  def getKeyIndex(map: MapData, equal: (ArrayData, Int) => Boolean): Int = {
    var index = -1
    var i = 0
    val keys = map.keyArray()
    while(index == -1 && i < map.numElements()) {
      if (equal(keys, i)) {
        index = i
      }
      i += 1
    }
    index
  }
}
