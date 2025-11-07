package com.sparkutils.quality.impl.mapLookup

import com.sparkutils.quality.MapLookups
import com.sparkutils.quality.QualityException.qualityException
import com.sparkutils.quality.impl.RuleRegistrationFunctions
import org.apache.spark.sql.ShimUtils.{column, expression}
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.types.DataType


object MapLookup {
  /**
   * For withColumn / select usage, the map generation and lookup expressions must be of the same type
   */
  def apply(mapLookupName: Column, lookupKey: Column, mapLookups: MapLookups): Column = {
    column(apply(expression(mapLookupName), expression(lookupKey), mapLookups))
  }

  def apply(mapLookupName: Expression, lookupKey: Expression, mapLookups: MapLookups): MapLookupExpression = {
    val id = RuleRegistrationFunctions.getString(mapLookupName) // Must be hard coded, can't give a dynamic data type otherwise it will fail at runtime not analysis
    val (bv, dt) = mapLookups.getOrElse(id, mapDoesNotExist(id))

    MapLookupExpression(id, lookupKey, bv, dt)
  }

  private[mapLookup] def mapDoesNotExist(map: String) = qualityException(s"The map: $map, does not exist in the provided MapLookups")

}

/**
 * Returns a value when the lookup is present with the correct value type, or Null when not throws if the table is not present
 * @param mapId the name of the map entry / dataframe the lookupmap belongs to
 * @param child the expression to lookup
 * @param arrayMap the lookup broadcast maps
 */
@ExpressionDescription(
  usage = "_FUNC_(content to lookup, bloomFilterName) - Returns either the lookup value or Null when not present",
  examples = """
    Examples:
      > SELECT _FUNC_('a thing that might be there', 'otherDataset');
       0.9
  """,
  since = "0.0.1")
case class MapLookupExpression(mapId: String, child: Expression, arrayMap: MapData, dataType: DataType) extends
  MapLookupExpressionBase[MapData] {

  protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)

  override def mapData(t: MapData): MapData = t
}
