package com.sparkutils.quality.impl.mapLookup

import com.sparkutils.quality.MapLookups
import com.sparkutils.quality.QualityException.qualityException
import com.sparkutils.quality.impl.RuleRegistrationFunctions
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.ShimUtils.{column, expression}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, UnaryExpression}
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, SparkSession}


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
case class MapLookupExpression(mapId: String, child: Expression, arrayMap: Broadcast[MapData], dataType: DataType) extends
  UnaryExpression with MapLookupExpressionBase[Broadcast[MapData]] {

  protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)

  override def mapData(t: Broadcast[MapData]): MapData = t.value
}
