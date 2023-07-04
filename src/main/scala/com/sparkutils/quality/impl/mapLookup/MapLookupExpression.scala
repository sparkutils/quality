package com.sparkutils.quality.impl.mapLookup

import com.sparkutils.quality.MapLookups
import com.sparkutils.quality.QualityException.qualityException
import com.sparkutils.quality.impl.{MapUtils, RuleRegistrationFunctions}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, IsNotNull, Literal, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.unsafe.types.UTF8String

object MapLookup {
  /**
    * For withColumn / select usage, the map generation and lookup expressions must be of the same type
    */
  def apply(mapLookupName: Column, lookupKey: Column, mapLookups: MapLookups): Column = {
    new Column(apply(mapLookupName.expr, lookupKey.expr, mapLookups))
  }

  def apply(mapLookupName: Expression, lookupKey: Expression, mapLookups: MapLookups): MapLookupExpression = {
    val id = RuleRegistrationFunctions.getString(mapLookupName) // Must be hard coded, can't give a dynamic data type otherwise it will fail at runtime not analysis
    val (bv, dt) = mapLookups.getOrElse(id, mapDoesNotExist(id))

    MapLookupExpression(id, lookupKey, bv, dt)
  }

  private[mapLookup] def mapDoesNotExist(map: String) = qualityException(s"The bloom filter: $map, does not exist in the provided bloomMap")

  private[mapLookup] def mapDoesNotExistJ(map: String) = () => mapDoesNotExist(map)
}

/**
  * Returns a value when the lookup is present with the correct value type, or Null when not throws if the table is not present
  * @param mapId the name of the map entry / dataframe the lookupmap belongs to
  * @param child the expression to lookup
  * @param arrayMap the lookup broadcast maps  *
  */
@ExpressionDescription(
  usage = "_FUNC_(content to lookup, bloomFilterName) - Returns either the lookup value or Null when not present",
  examples = """
    Examples:
      > SELECT _FUNC_('a thing that might be there', 'otherDataset');
       0.9
  """,
  since = "1.5.0")
case class MapLookupExpression(mapId: String, child: Expression, arrayMap: Broadcast[MapData], dataType: DataType) extends UnaryExpression with CodegenFallback {
  lazy val theMap = MapUtils.toScalaMap(arrayMap.value, child.dataType, dataType)

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

  override def sql: String = s"(mapLookup($mapId, ${child.sql}))"

  protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)
}
