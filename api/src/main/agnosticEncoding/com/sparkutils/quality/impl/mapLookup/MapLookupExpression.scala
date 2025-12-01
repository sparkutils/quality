package com.sparkutils.quality.impl.mapLookup

import com.sparkutils.quality.QualityException.qualityException
import com.sparkutils.quality.impl.RuleRegistrationFunctions
import com.sparkutils.quality.impl.mapLookup.MapLookupFunctions.MapLookups
import org.apache.spark.sql.ShimUtils.{column, expression}
import org.apache.spark.sql.{Column, ShimUtils, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionDescription}
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataType, MapType}


object MapLookup {
  /**
   * For withColumn / select usage, the map generation and lookup expressions must be of the same type
   */
  def apply(mapLookupName: Column, lookupKey: Column, mapLookups: MapLookups): Column =
    ShimUtils.callFunction("map_lookup", mapLookupName, lookupKey, col(mapLookups))

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
  since = "0.2.0")
// BinaryExpression needed so ResolveExecuteImmediate can pickup arrayMap and convert from VariableReference to Literal
case class MapLookupExpression(mapId: String, child: Expression, arrayMap: Expression) extends BinaryExpression with
  MapLookupExpressionBase[Expression] {

  override def left: Expression = child
  override def right: Expression = arrayMap

  protected def withNewChildrenInternal(left: Expression, right: Expression): Expression = copy(child = left, arrayMap = right)

  override def mapData(t: Expression): MapData = t.eval().asInstanceOf[MapData]

  override val dataType: DataType = arrayMap.dataType.asInstanceOf[MapType].valueType
}