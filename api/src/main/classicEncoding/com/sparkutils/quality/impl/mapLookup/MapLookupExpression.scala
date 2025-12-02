package com.sparkutils.quality.impl.mapLookup

import com.sparkutils.quality.MapLookups
import com.sparkutils.quality.QualityException.qualityException
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.ShimUtils.{column, expression}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, UnaryExpression}
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, SparkSession}

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

    MapLookupExpression(id, lookupKey, SparkSession.active.sparkContext.broadcast(bv), dt)
  }

  private[mapLookup] def mapDoesNotExist(map: String) = qualityException(s"The map: $map, does not exist in the provided MapLookups")

}
