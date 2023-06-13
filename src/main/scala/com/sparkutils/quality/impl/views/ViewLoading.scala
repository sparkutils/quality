package com.sparkutils.quality.impl.views

import com.sparkutils.quality.impl.Validation
import com.sparkutils.quality.{DataFrameLoader, Id}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, SparkSession}

import scala.collection.mutable

case class TokenWithFilter(token: String, filter: Option[String])

/**
 * Represents a configuration row for view loading
 * @param name the view name, this will be used to manage dependencies
 * @param source either a loaded DataFrame or an sql to run against the catalog
 */
case class ViewConfig(name: String, source: Either[DataFrame, String])

/**
 * Underlying row information converted into a ViewConfig with the following logic:
 *
 * a) if token is specified sql is ignored
 * b) if token is null sql is used
 * c) if both are null the row will not be used
 */
private[views] case class ViewRow(name: String, token: Option[String], filter: Option[String], sql: Option[String])

trait ViewLoading {

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
               ): (Seq[ViewConfig], Set[String]) = {
    import viewDF.sparkSession.implicits._

    val filtered =
      viewDF.filter(
        ruleSuiteIdColumn === ruleSuiteId.id && ruleSuiteVersionColumn === ruleSuiteId.version)
      .select(name.as("name"), token.as("token"), filter.as("filter"), sql.as("sql"))
      .as[ViewRow]

    val rejects = filtered.filter("token is null and sql is null").select("name").as[String].collect().toSet

    val rows = filtered.filter("not(token is null and sql is null)").collect().toSeq
    (
      rows.map{ vr =>
        ViewConfig( vr.name,
          vr.token.fold[Either[DataFrame,String]]( Right(vr.sql.get ) ){ token =>
            val df = loader.load(token)
            Left(vr.filter.fold(df)( df.filter(_) ))
          }
        )
      }, rejects)
  }

  /**
   * Attempts to load all the views present in the config.  If a view is already registered in that name it will be replaced.
   * @param viewConfigs
   * @return the names of views which have been replaced
   */
  def loadViews(viewConfigs: Seq[ViewConfig]): ViewLoadResults = {

    // assume some will not load, attempt count is used to stop cycles, 2x should be enough
    var attemptCount = 0
    var done = false
    var leftToProcess = viewConfigs.map(t => t.name -> t).toMap

    val replaced = mutable.Set.empty[String]
    // if we are missing a relation and the name is declared try to load that first
    var mapOf = leftToProcess
    var processed = mutable.Set.empty[String]

    def processView(viewPair: ViewConfig): Unit =
      if (attemptCount < (viewConfigs.size * 2)) {
        try {
          val (name, config) = (viewPair.name, viewPair)
          if (Validation.defaultViewLookup(name)) {
            replaced += name
          }

          config.source.fold(t => t
            , sql =>
              SparkSession.active.sql(sql)
          ).createOrReplaceTempView(name)

          // it worked, remove it
          leftToProcess = leftToProcess - name
          processed = processed + name
        } catch {
          case ae: AnalysisException =>
            ae.plan.fold(throw ae) {
              plan =>
                val c =
                  plan.collect {
                    case ur: UnresolvedRelation =>
                      if (mapOf.contains(ur.name)) { // TODO quoted
                        attemptCount += 1
                        processView(mapOf(ur.name))
                      } else // not one we can actually do anything about
                        throw ae
                  }

                if (c.isEmpty) {
                  throw ae // not what we expected
                }
            }
        }
      }

    while((attemptCount < (viewConfigs.size * 2)) && !done){
      attemptCount += 1
      leftToProcess.headOption.fold {
        done = true;
      } { p =>
        processView(p._2)
      }
    }
    ViewLoadResults(replaced.toSet, !done)
  }
}

case class ViewLoadResults( replaced: Set[String], failedToLoadDueToCycles: Boolean = false)

object ViewLoader {
  def tableOrViewNotFound(ae: AnalysisException): Either[Throwable, Set[String]] =
    ae.plan.fold[Either[Throwable, Set[String]]](Left(ae)) {
      plan =>
        val c =
          plan.collect {
            case ur: UnresolvedRelation =>
              ur.name
          }

        if (c.isEmpty)
          Left(ae) // not what we expected
        else
          Right(c.toSet)
    }
}