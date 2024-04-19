package com.sparkutils.qualityTests
import com.sparkutils.quality.{DataFrameLoader, Id, loadViewConfigs, loadViews}
import com.sparkutils.quality.impl.views.{MissingViewAnalysisException, ViewConfig, ViewLoader, ViewLoaderAnalysisException}
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{DataFrame, QualitySparkUtils, ShimUtils, AnalysisException => SAE}
import org.junit.Assert.fail
import org.junit.Test

class ViewLoaderTest extends TestUtils {

  val loader = new DataFrameLoader {
    override def load(token: String): DataFrame = {
      import sparkSession.implicits._
      token match {
        case "names" | "names2" => Seq(X2("rog","dodge"), X2("rog","nododge"), X2("rod","nojane"), X2("freddy", "jane")).toDF()
        case "ages" | "ages2" => Seq(X2("dodge",12), X2("nododge",45), X2("nojane", 50), X2("jane", 24)).toDF()
      }
    }
  }

  val config =
    Seq(
      ViewRow(Id(1,1),"joined", None, None, Some("select * from names n left outer join ages a on n.b = a.a")),
      ViewRow(Id(1,1),"names", Some("names"), None, None),
      ViewRow(Id(100,1),"nameLess", Some("names"), Some("b <> 'jane'"), None),
      ViewRow(Id(1,1),"ages", Some("ages"), Some("b > 12"), None),
      ViewRow(Id(1,1),"bad", None, None, None)
    )

  def doViewLoadingTest(res: (Seq[ViewConfig], Set[String])): Unit ={
    val (viewConfigs, failed) = res

    assert(failed == Set("bad"), "should have had 'bad' as both token and sql are None")
    assert(viewConfigs.size == 3)
    val sorted = viewConfigs.sortBy(_.name)
    assert(sorted(0).source.isLeft)
    assert(sorted(2).source.isLeft)
    assert(sorted(1).source.isRight)

    assert(sorted(0).source.left.get.filter("b = 12").isEmpty)
  }

  @Test
  def testConfigLoading(): Unit = {
    import sparkSession.implicits._

    val res = loadViewConfigs(loader, config.toDF(), expr("id.id"), expr("id.version"), Id(1,1),
      col("name"),col("token"),col("filter"),col("sql")
    )

    doViewLoadingTest(res)
  }

  @Test
  def testConfigLoadingWithoutIds(): Unit = {
    import sparkSession.implicits._

    val res = loadViewConfigs(loader, config.filterNot(_.id == Id(100,1)).map(_.to2).toDF(),
      col("name"),col("token"),col("filter"),col("sql")
    )

    doViewLoadingTest(res)
  }

  @Test
  def testViewLoads(): Unit = {
    import sparkSession.implicits._

    val (viewConfigs, _) = loadViewConfigs(loader, config.toDF(), expr("id.id"), expr("id.version"), Id(1,1),
      col("name"),col("token"),col("filter"),col("sql")
    )
    val results = loadViews(viewConfigs)
    assert(results.replaced.isEmpty)
    assert(!results.failedToLoadDueToCycles)
    assert(results.notLoadedViews.isEmpty)

    val (viewConfigs2, _) = loadViewConfigs(loader, config.toDF(), expr("id.id"), expr("id.version"), Id(1,1),
      col("name"),col("token"),col("filter"),col("sql")
    )
    val results2 = loadViews(viewConfigs2)
    assert(results2.replaced == Set("names","ages","joined"))
  }

  @Test
  def testViewLoadsFailedAsJoinsNotPresent(): Unit = {
    import sparkSession.implicits._

    val config =
      Seq(
        ViewRow(Id(1,1),"joined", None, None, Some("select * from names43 n left outer join ages353 a on n.b = a.a"))
      )

    val (viewConfigs, _) = loadViewConfigs(loader, config.toDF(), expr("id.id"), expr("id.version"), Id(1,1),
      col("name"),col("token"),col("filter"),col("sql")
    )

    try {
      loadViews(viewConfigs)
      fail("should have thrown an AnalysisException as neither names43 nor ages353 are present")
    } catch {
      case MissingViewAnalysisException(cause, message, viewName, sql, missingRelationNames) =>
        val r = ShimUtils.tableOrViewNotFound(cause).getOrElse(throw cause)
        assert(r.isRight)
        // spark 3+ gives two, 2 only the first
        val notFound = r.right.get

        val expectedNotFoundSet =
          if (sparkVersion == "2.4")
            Set("names43")
          else
            Set("names43","ages353")

        assert(notFound == expectedNotFoundSet)

        assert(missingRelationNames == expectedNotFoundSet)
        assert(viewName == "joined")
        assert(sql == config(0).sql.get)
    }
  }

  @Test
  def testViewLoadsFailedAsInfinite(): Unit = {
    import sparkSession.implicits._

    val config =
      Seq(
        ViewRow(Id(1,1),"le1", None, None, Some("select * from le2")),
        ViewRow(Id(1,1),"le2", None, None, Some("select * from le1"))
      )

    val (viewConfigs, _) = loadViewConfigs(loader, config.toDF(), expr("id.id"), expr("id.version"), Id(1,1),
      col("name"),col("token"),col("filter"),col("sql")
    )

    val res = loadViews(viewConfigs)
    assert(res.failedToLoadDueToCycles)
    assert(res.notLoadedViews == Set("le1","le2"))
  }

  @Test
  def testViewLoadsThatNeedQuoting(): Unit = {
    import sparkSession.implicits._

    val config =
      Seq(
        ViewRow(Id(1,1),"le2", None, None, Some("select * from `le-21`"))
      )

    val (viewConfigs, _) = loadViewConfigs(loader, config.toDF(), expr("id.id"), expr("id.version"), Id(1,1),
      col("name"),col("token"),col("filter"),col("sql")
    )
    try {
      loadViews(viewConfigs)
      fail("should have thrown an AnalysisException as `le-21` is not present")
    } catch {
      case MissingViewAnalysisException(cause, message, viewName, sql, missingRelationNames) =>
        val r = ShimUtils.tableOrViewNotFound(cause).getOrElse(throw cause)
        assert(r.isRight)
        // sparks below 3.2 don't quote.
        if (sparkVersion.replace(".","").toInt < 32)
          assert(missingRelationNames == Set("le-21"))
        else
          assert(missingRelationNames == Set("`le-21`"))

        assert(viewName == "le2")
    }
  }


  @Test
  def testViewLoadsThatDontParse(): Unit = {
    import sparkSession.implicits._

    val config =
      Seq(
        ViewRow(Id(1,1),"joined2", None, None, Some("select * from names2 n left outer join ages2 a on n.b probably wont ever equal a.a")),
        ViewRow(Id(1,1),"names2", Some("names2"), None, None),
        ViewRow(Id(1,1),"ages2", Some("ages2"), Some("b > 12"), None)
      )

    val (viewConfigs, _) = loadViewConfigs(loader, config.toDF(), expr("id.id"), expr("id.version"), Id(1,1),
      col("name"),col("token"),col("filter"),col("sql")
    )
    try {
      loadViews(viewConfigs)
      fail("should have thrown an AnalysisException as `le-21` is not present")
    } catch {
      case ViewLoaderAnalysisException(cause, message, viewName, sql) =>
        val r = ShimUtils.tableOrViewNotFound(cause).getOrElse(throw cause)
        assert(r.isLeft)
        // spark 3+ gives two, 2 only the first
        assert(cause.getMessage.contains("probably"))
        assert(viewName == "joined2")
    }
  }

}

case class X2[A,B](a: A, b: B)
case class ViewRow(id: Id, name: String, token: Option[String], filter: Option[String], sql: Option[String]) {
  def to2: ViewRow2 = ViewRow2(name, token, filter, sql)
}
case class ViewRow2(name: String, token: Option[String], filter: Option[String], sql: Option[String])