package com.sparkutils.quality.impl.mapLookup

import com.sparkutils.quality.impl.util.{Config, Row}
import org.apache.spark.sql.DataFrame

/**
 * Represents a configuration row for map loading
 * @param name the map name
 * @param source either a loaded DataFrame or an sql to run against the catalog
 * @param key must be a valid expression representing the map key to be taken from the source
 * @param value must be a valid expression representing the map value to be taken from the source
 */
case class MapConfig(override val name: String, override val source: Either[DataFrame, String], key: String, value: String) extends Config(name, source)

/**
 * Underlying row information converted into a MapConfig with the following logic:
 *
 * a) if token is specified sql is ignored
 * b) if token is null sql is used
 * c) if both are null the row will not be used
 */
case class MapRow(override val name: String, override val token: Option[String],
                                  override val filter: Option[String], override val sql: Option[String], key: String, value: String)
  extends Row(name, token, filter, sql)
