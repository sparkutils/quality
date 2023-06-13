package com.sparkutils.qualityTests
import com.sparkutils.quality.{DataFrameLoader, Id}
import com.sparkutils.quality.impl.views.{ViewLoader, ViewLoading}
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{AnalysisException => SAE, DataFrame}
import org.junit.Assert.fail
import org.junit.Test

class ViewLoaderTest extends TestUtils with ViewLoading {

  val loader = new DataFrameLoader {
    override def load(token: String): DataFrame = {
      import sparkSession.implicits._
      token match {
        case "names" => Seq(X2("rog","dodge"), X2("rog","nododge"), X2("rod","nojane"), X2("freddy", "jane")).toDF()
        case "ages" => Seq(X2("dodge",12), X2("nododge",45), X2("nojane", 50), X2("jane", 24)).toDF()
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


  @Test
  def testConfigLoading(): Unit = {
    import sparkSession.implicits._

    val (viewConfigs, failed) = loadViewConfigs(loader, config.toDF(), expr("id.id"), expr("id.version"), Id(1,1),
      col("name"),col("token"),col("filter"),col("sql")
    )

    assert(failed == Set("bad"), "should have had 'bad' as both token and sql are None")
    assert(viewConfigs.size == 3)
    val sorted = viewConfigs.sortBy(_.name)
    assert(sorted(0).source.isLeft)
    assert(sorted(2).source.isLeft)
    assert(sorted(1).source.isRight)

    assert(sorted(0).source.left.get.filter("b = 12").isEmpty)
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
      case ae: SAE =>
        val r = ViewLoader.tableOrViewNotFound(ae)
        assert(r.isRight)
        assert(r.right.get == Set("names43","ages353"))
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
  }
}

case class X2[A,B](a: A, b: B)
case class ViewRow(id: Id, name: String, token: Option[String], filter: Option[String], sql: Option[String])