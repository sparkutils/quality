package com.sparkutils.qualityTests

import com.sparkutils.quality._
import com.sparkutils.quality.impl.mapLookup.MapLookupFunctions
import com.sparkutils.quality.impl.util.RuleSuiteGroupIOUtils
import com.sparkutils.qualityTests.RulesGen.{fModExpr, genRules1to1, genRulesMap}
import com.sparkutils.qualityTests.util.{SharedConnectTests, SharedPureConnectTests}
import com.sparkutils.testing.{ConnectionType, Sessions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, SaveMode, SparkSession}
import org.scalatest.Matchers

import scala.collection.immutable
import scala.concurrent.duration.Duration

object RulesGen extends SharedConnectTests {

  // only available on 3.5
  val replace = org.apache.spark.sql.functions.udf((source: String, against: String, withWhat: String) =>
    source.replace(against, withWhat))

  val f_bucket_size = 40
  val fModExpr = s"if(f = '*', 0, hash(f) % $f_bucket_size)"

  def genRules1to1(s: SparkSession, withF: Boolean = false) = {

    val d = s.read.option("header",true).csv("server/src/test/resources/20k_rule_suite.csv")
    val cols = d.columns.toSet -- Set("k","l", "id") -- (
      if (withF)
        Set("f")
      else
        Set.empty
    )
    def exprOf(name: String): String = s"if($name = '*', 'remove', '$name = ''' || $name || '''')"
    val ruleGen = cols.toSeq.map(exprOf).mkString(" || ' and ' || ")
    val ruleDS = d.select(Seq(
      replace(
        replace(expr(ruleGen), lit("remove and "), lit("")),
        lit("and remove"), lit("")
      ).as("_1"),
      expr("'struct(''' ||  k || ''',''' || l || ''')'").
        as("_2"), expr("id").cast(IntegerType).as("_3")) ++ (
      if (withF)
        Seq(expr("f"), expr(fModExpr).as("f_mod"))
      else
        Seq.empty
    ) :_*)
    ruleDS
  }

  def genRulesMap(s: SparkSession, withId: Boolean = false) = {
    val d = s.read.option("header",true).csv("server/src/test/resources/20k_rule_suite.csv")
    val cols = (d.columns.toSet -- Set("k","l","id")).toSeq.sorted
    def exprOf(name: String): String = s"if($name = '*', 'remove', '$name')"
    val ruleGen = cols.map(exprOf).mkString(" || ' , ' || ")

    def filterOf(name: String): String = s"if($name = '*', '$name = ''*''', '$name != ''*''')"
    val filterGen = cols.map(filterOf).mkString(" || ' and ' || ")

    val ruleDS = d.select(
      Seq(
        concat(lit("struct("),
          replace(
            replace(expr(ruleGen), lit("remove , "), lit("")),
              lit(", remove"), lit("")
          ), lit(")")).as("trigger"),
        expr("'struct(k, l)'").
          as("output"),
        (lit(1000) -
          aggregate(array(cols.toSeq.map(name => expr(s"if($name = '*', 0, 1)")) :_*), lit(0), (a, b) => a + b)
          ).as("salience"),
        expr(filterGen).as("filter")
      ) ++ (
        if (withId)
          Seq(expr("id"))
        else
          Seq.empty
      ) : _*
    ).distinct()
    ruleDS
  }

  def main(args: Array[String]): Unit = {
    val s = sparkSession
    //genRules1to1(s).write.mode(SaveMode.Overwrite).option("header",true).csv(outputDir + "/1to1rules.csv")
    genRulesMap(s).write.mode(SaveMode.Overwrite).option("header",true).csv(outputDir + "/map_building.csv")
    // copy these over to resources/rules.csv
  }

  override val connectionType: ConnectionType = com.sparkutils.testing.ClassicOnly

  override def sessions: Sessions = createSparkSessions(connectionType)

}

class BigRules extends SharedPureConnectTests with Matchers {

  override val runWith: ConnectionType = com.sparkutils.testing.ClassicOnly

  def rules(dataSet: Dataset[(String, String, Int)]) = {
    val s = sparkSession

    //.write.mode(SaveMode.Overwrite).option("header",true).csv(outputDir + "/rules.csv")
    val rules = dataSet.orderBy("_3").collect().map{
      case (trigger, output, id) =>

        Rule(Id(id, 1), ExpressionRule(trigger),
              RunOnPassProcessor(1000 + id, Id(1040 + id, 1), OutputExpression(output)))
    }
    RuleSuite(Id(1,0), Seq(
      RuleSet(Id(50, 1), rules
      )))
  }

  def doRuleTest(ruleSuite: RuleSuite, typ: String, resultDataType: Option[DataType] = Some(
    StructType(Seq(
      StructField("k_out", StringType),
      StructField("l_out", StringType)
    )
  )), topLevelRunner: (RuleSuite, Option[DataType]) => Column = ruleEngineRunner(_, _), processor: DataFrame => DataFrame =
        _.select(expr("*"), expr("runner.result.*"))) = {
    var start = System.nanoTime()
    val s = sparkSession
    val d = s.read.option("header",true).csv("server/src/test/resources/20k_rule_suite.csv")
    val r = processor(d.select(expr("*"), topLevelRunner(ruleSuite, resultDataType).
      as("runner")))
    var end = System.nanoTime()

    println(s"$typ - took ${Duration.fromNanos(end - start).toSeconds}s to do logical plan")
    start = System.nanoTime()
    r.limit(1).write.format("noop").mode(SaveMode.Overwrite).save()
    end = System.nanoTime()
    val compilationEstimation = Duration.fromNanos(end - start)
    println(s"$typ - took ${compilationEstimation.toMinutes}m${compilationEstimation.toSeconds % 60}s to do a limit 1, closest to compile time")

    start = System.nanoTime()
    r.write.format("noop").mode(SaveMode.Overwrite).save()
    end = System.nanoTime()
    val fullDump = Duration.fromNanos(end - start)
    val processOf20kx20k = fullDump - compilationEstimation
    println(s"$typ - took ${fullDump.toMinutes}m${fullDump.toSeconds % 60}s to do full noop write, of which" +
      s" ${processOf20kx20k.toMinutes}m${processOf20kx20k.toSeconds % 60}s in processing 20kx20k")
    r
  }

  test("map run over the rules") {
    val s = sparkSession

    import s.implicits._

    val d = s.read.option("header",true).csv("server/src/test/resources/20k_rule_suite.csv")
    d.createOrReplaceTempView("mapsource")
    val conf = genRulesMap(s).as[(String, String, Int, String)].collect().zipWithIndex.map{
      case ((key, value, salience, filter),index) =>
        (MapRow(1, 1, "m"+index, None,
          filter = None, Some(s"select * from mapsource where $filter"), key, value),
          s"map_contains('m$index', $key, themaps)",
          s"map_lookup('m$index', $key, themaps)",
          salience
          )
    }
    val mapConfig =
      loadMapConfigs(new DataFrameLoader {
        override def load(token: String): DataFrame = ???
      }, conf.map(_._1).toSeq.toDF, Id(1,1))
    MapLookupFunctions.loadMaps(mapConfig._1, "themaps")

    // substitute the map names
    val rd = conf.map(t => (t._2, t._3, t._4)).toSeq.toDS
    val ruleSuite = rules(rd)

    val res = doRuleTest(ruleSuite, "map")
    val play = res.cache
    val count = play.count
    val withResultCount = play.where("k_out is not null").count
    withResultCount shouldBe count
    //res.show
    play.filter("k != k_out or l != l_out").
      count() shouldBe 0
  }

  test("map run over the rules - 20k with id") {
    val s = sparkSession

    import s.implicits._

    val d = s.read.option("header",true).csv("server/src/test/resources/20k_rule_suite.csv")
    d.createOrReplaceTempView("mapsource")
    val conf = genRulesMap(s).as[(String, String, Int, String)].collect().zipWithIndex.map{
      case ((key, value, salience, filter),index) =>
        (MapRow(1, 1, "m"+index, None,
          filter = None, Some(s"select * from mapsource where $filter"), key, value),
          s"map_contains('m$index', $key, themaps)",
          s"map_lookup('m$index', $key, themaps)",
          salience
        )
    }

    val mapConfig =
      loadMapConfigs(new DataFrameLoader {
        override def load(token: String): DataFrame = ???
      }, conf.map(_._1).toSeq.toDF, Id(1,1))
    MapLookupFunctions.loadMaps(mapConfig._1, "themaps")

    val maps = conf.map(c => c._1.key -> c._1.name).toMap

    val rd = genRulesMap(s, true).as[(String, String, Int, String, String)].collect().map{
      case (key, value, salience, filter, id) =>
        (
          s"map_contains('${maps(key)}', $key, 'themaps') and id = '$id'",// ",//
          value,
          salience
        )
    }.toSeq.toDS

    // substitute the map names
    val ruleSuite = rules(rd)

    val res = doRuleTest(ruleSuite, "map 20k with id")
    val play = res.cache
    val count = play.count
    val withResultCount = play.where("result is not null").count
    withResultCount shouldBe count
    //res.show
    play.filter("k != k_out or l != l_out").
      count() shouldBe 0
  }

  test("simple run over the rules 1:1") {
    val s = sparkSession
    import s.implicits._
    val res = doRuleTest(rules(genRules1to1(sparkSession).as[(String, String, Int)]), "1:1")
    val play = res.cache
    val count = play.count
    val withResultCount = play.where("k_out is not null").count
    withResultCount shouldBe count
  }

  test("grouped 129 simulation = via mod 50 on f - 20k total rules") {
    val s = sparkSession

    import s.implicits._

    val d = s.read.option("header",true).csv("server/src/test/resources/20k_rule_suite.csv")
    d.createOrReplaceTempView("mapsource")

    val b = "b = '.*?'"
    val rulesRaw = genRules1to1(sparkSession, true).as[(String, String, Int, String, Long)]

    val groups = rulesRaw.collect().zipWithIndex.map{
      case ((key, value, salience, facility, f_mod), index) =>
        val facIdMod =
          if (f_mod == 0L) // the * case
            ""
          else
            s" and $fModExpr = $f_mod "
        (
          key,
          index,
          s"$key $facIdMod",
          key.replaceAll(s"$b and","").replaceAll(s"and $b",""),
          s"$key and f = '$facility' ", // also works *'s
          value,
          index
        )
    }.toSeq

    //val topLevel = groups.groupBy(_._1)

    val (only1, multiple) = groups.groupBy(_._3).partition(_._2.size == 1)
    type groupy = immutable.Iterable[((String, Iterable[(String, Int, String, String, String, String, Int)]), Int)]

    val reGrouped: groupy = only1.values.flatten.groupBy(_._4).zipWithIndex // no facility and no trade bal id

    val withFacility: groupy  = multiple.zipWithIndex // with facility


    // this destroys salience and is only used to suggest what kind of performance difference could be gained by this kind of bucketing

    def nested(group: groupy, idOffset: Int) = {
      group.map{
        case ((top, itr), index) =>
          val rs = rules(itr.map(t => (t._5,t._6,t._7)).toSeq.toDS).
            copy(id = Id(idOffset + index, 1))
          val rsv = register_rule_suite(rs)
          (top, s"rule_engine_runner($rsv)", idOffset + index)
      }
    }

    val therules = (nested(withFacility, 1000) ++ nested(reGrouped, withFacility.size + 200)).toSeq

    val ruleSuite = rules(therules.toDS)

    val res = doRuleTest(ruleSuite, "192 simulation 100x400 max evals", resultDataType = None)
    val play = res.cache
    val count = play.count
    print(s"got ${play.where("result is not null").count} non null results out of $count total rows")
    play.show
    play.filter("k != result.k or l != result.l").
      count() shouldBe 0
  }


  test("grouped 129 via top level boolean grouping") {
    // requires you generate the rules grouping first, this just proves it should work.
    // do so for any changes (e.g. different grouping approaches) by breakpointing RuleEngineRunner
    // on withNewChildren to get resolved expressions and debug 1:1 test then evaluate
    // SuiteBuilder.build("grouped", newChildren, this)
    val group = RuleSuiteGroupIOUtils.fromFile("./grouped")
    val s = sparkSession

    val d = s.read.option("header",true).csv("server/src/test/resources/20k_rule_suite.csv")
    register_rule_suite_group(group, "the_group")
    /*group.ruleSuites.foreach{
      case (id, rs) =>
        register_rule_suite(rs, s"ruleSuite${id.id}")
    }*/

    // when running as ruleRunner all of Id(0,0) find matches, alas multiple matches (more than 2 for some), as
    // such it's possible the groups are too aggressive, but collect *should* still allow capturing

/// s"rule_engine_runner(ruleSuite${index+1})"
    val res = doRuleTest(group.ruleSuites(Id(0,0)), "grouped 129 via top level boolean grouping", resultDataType = None)/*,
      topLevelRunner = collectRunner(_,_), _.select(expr("*"), expr("get(filter(runner.result, x -> x.ruleSuiteResults.overallResult = passed()), 0)").as("thepackage")).
        select(expr("*"),expr("thepackage.*")))*/
    val play = res.cache
    val count = play.count
    print(s"got ${play.where("result is not null").count} non null results out of $count total rows")
    play.show
    play.filter("(result.col1 is null) or (k != result.col1) or (l != result.col2) or (result.col2 is null)").
      count() shouldBe 0
  }

  // just a test to spit out rules from grouped
  test("dummy degrouper") {
    val group = RuleSuiteGroupIOUtils.fromFile("./grouped")
    val s = sparkSession
    import s.implicits._
    import com.sparkutils.quality.implicits._

    group.ruleSuites.foldLeft(Seq.empty[CombinedRuleSuiteRows]){
      case (cur, (id, rs)) =>
        ///combined_rows(rs).write.mode(SaveMode.Append).format("json").save(".target/full_grouped")
        cur :+ combined_rows(rs).head()
    }.toDS().coalesce(4).write.mode(SaveMode.Overwrite).format("json").save("target/full_grouped")
  }

  test("attempt to load rules json"){
    val s = sparkSession

    import s.implicits._
    import com.sparkutils.quality.implicits._
    val d = s.read.schema(frameless.TypedExpressionEncoder[CombinedRuleSuiteRows].schema).option("header",true).json("server/src/test/resources/20k_grouped.json")

    val rules = rule_suite_group(d.as[CombinedRuleSuiteRows])

    rules.ruleSuites.size > 4
  }
}
