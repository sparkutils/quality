package com.sparkutils.quality.impl.views

import com.sparkutils.quality.impl.util.ConfigLoader
import com.sparkutils.quality.{DataFrameLoader, Id}
import org.apache.spark.sql.{Column, DataFrame}


trait ViewLoading {

  import ViewLoader.{viewRowEncoder, factory}

  /**
   * Loads view configurations from a given DataFrame for ruleSuiteId.  Wherever token is present loader will be called and the filter optionally applied.
   * @return A tuple of ViewConfig's and the names of rows which had unexpected content (either token or sql must be present)
   */
  def loadViewConfigs(loader: DataFrameLoader, viewDF: DataFrame,
                      ruleSuiteIdColumn: Column,
                      ruleSuiteVersionColumn: Column,
                      ruleSuiteId: Id,
                      name: Column,
                      token: Column,
                      filter: Column,
                      sql: Column
                     ): (Seq[ViewConfig], Set[String]) =
    ConfigLoader.loadConfigs[ViewConfig, ViewRow](
      loader, viewDF,
      ruleSuiteIdColumn,
      ruleSuiteVersionColumn,
      ruleSuiteId,
      name,
      token,
      filter,
      sql
    )

  /**
   * Loads view configurations from a given DataFrame.  Wherever token is present loader will be called and the filter optionally applied.
   * @return A tuple of ViewConfig's and the names of rows which had unexpected content (either token or sql must be present)
   */
  def loadViewConfigs(loader: DataFrameLoader, viewDF: DataFrame,
                      name: Column,
                      token: Column,
                      filter: Column,
                      sql: Column
                     ): (Seq[ViewConfig], Set[String]) =
    ConfigLoader.loadConfigs[ViewConfig, ViewRow](
      loader, viewDF,
      name,
      token,
      filter,
      sql
    )

  /**
   * Attempts to load all the views present in the config.  If a view is already registered in that name it will be replaced.
   * @param viewConfigs
   * @return the names of views which have been replaced
   */
  def loadViews(viewConfigs: Seq[ViewConfig]): ViewLoadResults = ViewLoader.loadViews(viewConfigs)
}