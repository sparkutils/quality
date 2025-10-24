package com.sparkutils.quality.tests

import com.sparkutils.quality.Rule
import com.sparkutils.quality.impl.util.RuleModel.RuleSuiteMap
import com.sparkutils.quality._
import com.sparkutils.quality.impl.util.MetaRuleSetRow
import com.sparkutils.qualityTests.{RowTools, TestUtils}
import org.apache.spark.sql.ShimUtils.{column, expression}
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.{Column, DataFrame}
import org.junit.Test
import org.scalatest.FunSuite

class MetaRuleSetTest extends FunSuite with TestUtils {

  def ruleSet(id: Int, arg: String, exp: String) = MetaRuleSetRow(1,1,id,id*2,"",
    s"$arg -> $exp")

  def ruleFilter(filter: String) = MetaRuleSetRow(1,1,1,2,filter,"arg -> arg")

  def evalMeta(tup: (MetaRuleSetRow, String, Int, Rule)) =
    assert(tup._1.createGenerator( _ => tup._3).withNameAndOrd(tup._2) == tup._4)

  def evalFilter(tup: (MetaRuleSetRow, DataFrame, Set[String])) =
    assert(tup._1.filterColumns(tup._2) == tup._3)

  @Test
  def regExpTest: Unit = evalCodeGens {
    val rs = Seq(
      (ruleSet(1, "arg", "exp(arg)"), "col1", 1, Rule(Id(2, 2), ExpressionRule("exp(col1)") )),
      (ruleSet(2, "arg", "exp(argument, arg)"), "col1", 2, Rule(Id(4, 4), ExpressionRule("exp(argument, col1)") )),
      (ruleSet(2, "arg", "exp(arg, argument, arg)"), "col1", 10, Rule(Id(12, 4), ExpressionRule("exp(col1, argument, col1)") )),
      (ruleSet(4, "arg", "exp(1+arg*othercol, argument, arg)"), "col1", 20, Rule(Id(24, 8), ExpressionRule("exp(1+col1*othercol, argument, col1)") ))
    )
    rs.foreach(evalMeta)
  }

  val persons = Seq(Person("Amy", 21, None))

  @Test
  def filterExpTest: Unit = evalCodeGens {
    import sparkSession.implicits._
    import org.apache.spark.sql.functions.col
    // force the name field to be non-nullable
    val df = sparkSession.createDataset(persons).toDF.withColumn("name", column(AssertNotNull(expression(col("name")))))

    val rs = Seq(
      (ruleFilter("name = 'name'"), df, Set("name") ),
      (ruleFilter("datatype = 'STRING' "), df, Set("name", "fren") ),
      (ruleFilter("nullable = true"), df, Set("fren") ),
      (ruleFilter("datatype = 'BIGINT' "), df, Set("age") )
    )
    rs.foreach(evalFilter)
  }

  def doFullLoadTest(transform: DataFrame => DataFrame, columnFilter: String): Unit = evalCodeGens {
    import sparkSession.implicits._
    import org.apache.spark.sql.functions.col
    // force the name field to be non-nullable
    val df = sparkSession.createDataset(persons).toDF.withColumn("name", column(AssertNotNull(expression(col("name")))))

    val msr = MetaRuleSetRow(1,2,3,4, columnFilter, "col -> isNull(col)")

    val metaRuleDF = sparkSession.createDataset(Seq(msr)).toDF
    val themap = readMetaRuleSetsFromDF(metaRuleDF,
      col("columnFilter"),
      col("ruleExpr"),
      col("ruleSetId"),
      col("ruleSetVersion"),
      col("ruleSuiteId"),
      col("ruleSuiteVersion"))

    val reread = themap.head._2.head
    assert(reread == msr)

    val origRuleSuiteMap: RuleSuiteMap = Map(Id(1,2) -> RuleSuite(Id(1,2), Seq.empty))

    val expected: RuleSuiteMap = Map(Id(1,2) -> RuleSuite(Id(1,2), Seq(RuleSet(Id(3,4), Seq(Rule(Id(8, 4), ExpressionRule("isNull(fren)")))))))

    val newRuleSuiteMap = integrateMetaRuleSets(df, origRuleSuiteMap, themap, _ => 5, transform)
    assert(newRuleSuiteMap == expected)
  }

  @Test
  def fullLoadTest: Unit = doFullLoadTest(identity, "nullable = true")

  import org.apache.spark.sql.functions.expr
  @Test
  def transformLoadTest: Unit = doFullLoadTest(_.withColumn("newfield", expr("IF(name='fren', 1, 120)")), "newfield = 1")

}

case class Person(name: String, age: Long, fren: Option[String])