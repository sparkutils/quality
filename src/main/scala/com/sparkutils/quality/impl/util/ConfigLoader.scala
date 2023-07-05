package com.sparkutils.quality.impl.util

import com.sparkutils.quality.{DataFrameLoader, Id}
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, Dataset, Encoder, functions}


/**
 * Represents a configuration row for view loading
 *
 * @param name the view name, this will be used to manage dependencies
 * @param source either a loaded DataFrame or an sql to run against the catalog
 */
class Config(val name: String, val source: Either[DataFrame, String])

/**
 * Underlying row information converted into a ViewConfig with the following logic:
 *
 * a) if token is specified sql is ignored
 * b) if token is null sql is used
 * c) if both are null the row will not be used
 */
class Row(val name: String, val token: Option[String], val filter: Option[String], val sql: Option[String])

/**
 * Creates a config object of the correct type
 * @tparam T
 * @tparam R
 */
trait ConfigFactory[T <: Config, R <: Row] {
  def create(base: Config, row: R): T
}

protected[quality] object ConfigLoader {
  /**
   * Loads view configurations from a given DataFrame for ruleSuiteId.  Wherever token is present loader will be called and the filter optionally applied.
   * @return A tuple of ViewConfig's and the names of rows which had unexpected content (either token or sql must be present)
   */
  def loadConfigs[T <: Config, R <: Row : Encoder](loader: DataFrameLoader, configDF: DataFrame,
                      ruleSuiteIdColumn: Column,
                      ruleSuiteVersionColumn: Column,
                      ruleSuiteId: Id,
                      name: Column,
                      token: Column,
                      filter: Column,
                      sql: Column,
                      extra: Column *
                     )(implicit factory: ConfigFactory[T,R]): (Seq[T], Set[String]) = {
    val filtered =
      configDF.filter(
        ruleSuiteIdColumn === ruleSuiteId.id && ruleSuiteVersionColumn === ruleSuiteId.version)
        .select(Seq(name.as("name"), token.as("token"), filter.as("filter"), sql.as("sql")) ++ extra :_ *)
        .as[R]

    loadConfigs[T, R](loader, filtered)
  }

  /**
   * Loads view configurations from a given DataFrame.  Wherever token is present loader will be called and the filter optionally applied.
   * @return A tuple of ViewConfig's and the names of rows which had unexpected content (either token or sql must be present)
   */
  def loadConfigs[T <: Config, R <: Row : Encoder](loader: DataFrameLoader, configDF: DataFrame,
                      name: Column,
                      token: Column,
                      filter: Column,
                      sql: Column,
                      extra: Column *
                     )(implicit factory: ConfigFactory[T,R]): (Seq[T], Set[String]) = {
    val filtered =
      configDF
        .select(Seq(name.as("name"), token.as("token"), filter.as("filter"), sql.as("sql")) ++ extra :_ *)
        .as[R]

    loadConfigs[T, R](loader, filtered)
  }

  /**
   * Perform the actual load against a pre-prepared dataset
   * @return A tuple of ViewConfig's and the names of rows which had unexpected content (either token or sql must be present)
   */
  protected[quality] def loadConfigs[T <: Config, R <: Row](loader: DataFrameLoader, filtered: Dataset[R])(implicit factory: ConfigFactory[T,R]): (Seq[T], Set[String]) = {
    val rejects = {
      import filtered.sparkSession.implicits._
      filtered.filter("token is null and sql is null").select("name").as[String].collect().toSet
    }

    val rows = filtered.filter("not(token is null and sql is null)").collect().toSeq
    (
      rows.map{ vr =>
        factory.create(
          new Config( vr.name,
            vr.token.fold[Either[DataFrame,String]]( Right(vr.sql.get ) ){ token =>
              val df = loader.load(token)
              Left(vr.filter.fold(df)( df.filter(_) ))
            }
          ), vr)
      }, rejects)
  }

}
