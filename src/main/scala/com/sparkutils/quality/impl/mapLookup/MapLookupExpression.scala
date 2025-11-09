package com.sparkutils.quality.impl.mapLookup

import com.sparkutils.quality.impl.MapUtils
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.types.DataType


trait MapLookupExpressionBase[T] extends Expression with CodegenFallback {

  val mapId: String
  val child: Expression
  val arrayMap: T
  val dataType: DataType

  def mapData(t: T): MapData

  lazy val theMap = MapUtils.toScalaMapKeysConverted(mapData(arrayMap), child.dataType, dataType)

  lazy val converter = CatalystTypeConverters.createToScalaConverter(child.dataType)

  override def eval(row: InternalRow): Any = {
    val eres = child.eval(row)
    if (eres == null)
      null
    else {
      // strings will be UTF8Strings, they are present in the map, only convert when that fails
      val res = theMap.get(eres).orElse( theMap.get({
        val converted = converter(eres)
        converted
      } )).getOrElse( null)
      if (res != null)
        res
      else
        null
    }
  }

  override def nullable: Boolean = true

  override def sql: String = s"(map_lookup($mapId, ${child.sql}))"
}
