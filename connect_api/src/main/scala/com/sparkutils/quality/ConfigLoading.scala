package com.sparkutils.quality

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col => scol}

/**
 * Configuration columns for View loading
 *
 * @param ruleSuiteIdColumn
 * @param ruleSuiteVersionColumn
 * @param name
 * @param token
 * @param filter
 * @param sql
 */
@SerialVersionUID(1L)
case class ViewConfigColumns( ruleSuiteId: Column = scol("ruleSuiteId"),
                              ruleSuiteVersion: Column = scol("ruleSuiteVersion"),
                              name: Column = scol("name"),
                              token: Column = scol("token"),
                              filter: Column = scol("filter"),
                              sql: Column = scol("sql")) extends Serializable

/**
 * Simple class for loading / using views
 * @param ruleSuiteId
 * @param ruleSuiteVersion
 * @param name
 * @param token
 * @param filter
 * @param sql
 */
@SerialVersionUID(1L)
case class ViewRow(ruleSuiteId: Int, ruleSuiteVersion: Int, name: String, token: Option[String], filter: Option[String], sql: Option[String])
  extends Serializable

object ViewRow {
  /**
   * Simple class for loading / using views
   * @param id
   * @param name
   * @param token
   * @param filter
   * @param sql
   */
  def apply(id: Id, name: String, token: Option[String], filter: Option[String], sql: Option[String]): ViewRow =
    ViewRow(id.id, id.version, name, token, filter, sql)
}


/**
 * Configuration columns for Map loading
 *
 * @param ruleSuiteIdColumn
 * @param ruleSuiteVersionColumn
 * @param name
 * @param token
 * @param filter
 * @param sql
 * @param key
 * @param value
 */
@SerialVersionUID(1L)
case class MapConfigColumns( ruleSuiteId: Column = scol("ruleSuiteId"),
                             ruleSuiteVersion: Column = scol("ruleSuiteVersion"),
                             name: Column = scol("name"),
                             token: Column = scol("token"),
                             filter: Column = scol("filter"),
                             sql: Column = scol("sql"),
                             key: Column = scol("key"),
                             value: Column = scol("value")) extends Serializable

/**
 * Simple class for loading / using maps
 * @param ruleSuiteId
 * @param ruleSuiteVersion
 * @param name
 * @param token
 * @param filter
 * @param sql
 * @param key
 * @param value
 */
case class MapRow(ruleSuiteId: Int, ruleSuiteVersion: Int, name: String, token: Option[String],
                  filter: Option[String], sql: Option[String], key: String, value: String)

object MapRow {
  /**
   * Simple class for loading / using maps
   * @param ruleSuiteId
   * @param ruleSuiteVersion
   * @param name
   * @param token
   * @param filter
   * @param sql
   * @param key
   * @param value
   */
  def apply(id: Id, name: String, token: Option[String],
            filter: Option[String], sql: Option[String], key: String, value: String): MapRow =
    MapRow(id.id, id.version, name, token, filter, sql, key, value)
}