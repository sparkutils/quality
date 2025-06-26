package com.sparkutils.qualityTests

import com.sparkutils.quality._
import com.sparkutils.quality.sparkless.{ProcessFunctions, Processor}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.qualityFunctions.jdbc.jdbcHelper
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Row, ShimUtils}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import org.scalatestplus.junit.JUnitRunner

import java.sql.{DriverManager, ResultSet}
import java.util.Properties
import scala.annotation.tailrec


@RunWith(classOf[JUnitRunner])
class JDBCToRowTest extends FunSuite with Matchers with BeforeAndAfterAll with TestUtils {


  var forceMutable = false
  var forceVarCompilation = true

  def forceProcessors[T](thunk: => T): T = {
    // use projections
    forceMutable = true
    var r = thunk
    // only do in compile
    if (inCodegen) {
      // use mutable projection compilation approach
      forceMutable = false
      forceVarCompilation = false
      r = thunk
      // use current vars
      forceMutable = false
      forceVarCompilation = true
      r = thunk
    }
    r
  }



  private val testData = Seq(
    TestOn("edt", "4251", 50),
    TestOn("otc", "4201", 40),
    TestOn("fi", "4251", 50),
    TestOn("fx", "4206", 60),
    TestOn("fxotc", "4201", 40),
    TestOn("eqotc", "4200", 60)
  )

  private val theRuleSuite = RuleSuite(Id(1, 1), Seq(
    RuleSet(Id(50, 1), Seq(
      Rule(Id(100, 1), ExpressionRule("if(product like '%otc%', account = '4201', subcode = 50)"))
    ))
  ))

  private val urlString = "jdbc:h2:mem:testdb"
  private final var connection: java.sql.Connection = _

  test("test the existence of the test data in h2") {
    val resultSet = connection.prepareStatement(s"select * from test.test_on;").executeQuery()

    def toCaseClass(resultSet: ResultSet): TestOn =
      TestOn(product = resultSet.getString("product"), account = resultSet.getString("account"), subcode = resultSet.getInt("subcode"))

    val data = mapToList(resultSet = resultSet, toCaseClass)

    data shouldBe testData
  }


  test("test with jdbc dqFactory in h2") {
    not2_4 {
      not_Cluster {
        evalCodeGensNoResolve {

          val resultSet = connection.prepareStatement(s"select * from test.test_on;").executeQuery()

          val dialect = JdbcDialects.get(urlString)

          val schema: StructType = JdbcUtils.getSchema(connection, resultSet, dialect)

          val helper = jdbcHelper(dialect = dialect, schema = schema)

          implicit val renc: AgnosticEncoder[Row] = ShimUtils.rowEncoder(schema)

          val dqFactory = ProcessFunctions.dqFactory[Row](theRuleSuite)

          val jdbcDqProcessor: Processor[ResultSet, RuleSuiteResult] = helper.wrapResultSet(dqFactory).instance

          val ruleResults = mapToLazyList(resultSet = resultSet, jdbcProcessor = jdbcDqProcessor)
          ruleResults.map(_.overallResult) shouldBe Seq(Passed, Passed, Passed, Failed, Passed, Failed)
        }
      }
    }
  }

  test("test with jdbc ruleEngineFactoryT in h2") {
    not2_4 {
      not_Cluster {
        evalCodeGensNoResolve {
          val resultSet = connection.prepareStatement(s"select * from test.test_on;").executeQuery()

          val dialect = JdbcDialects.get(urlString)

          val schema: StructType = JdbcUtils.getSchema(connection, resultSet, dialect)

          val helper = jdbcHelper(dialect = dialect, schema = schema)

          implicit val renc: AgnosticEncoder[Row] = ShimUtils.rowEncoder(schema)

          val DDL = "ARRAY<STRUCT<`transfer_type`: STRING, `account`: STRING, `product`: STRING, `subcode`: INTEGER >>"

          registerLambdaFunctions(Seq(
            LambdaFunction("account_row", "(transfer_type, account) -> named_struct('transfer_type', transfer_type, 'account', account, 'product', product, 'subcode', subcode)", Id(123, 23)),
            LambdaFunction("account_row", "transfer_type -> account_row(transfer_type, account)", Id(123, 24)),
            LambdaFunction("subcode", "(transfer_type, sub) -> updateField(account_row(transfer_type, account), 'subcode', sub)", Id(123, 25))
          ))


          import sparkSession.implicits._

          val expressionRules = Seq((ExpressionRule("product = 'edt' and subcode = 40"), RunOnPassProcessor(1000, Id(1040,1),
            OutputExpression("array(account_row('from'), account_row('to', 'other_account1'))"))),
            (ExpressionRule("product like '%fx%'"), RunOnPassProcessor(1000, Id(1042,1),
              OutputExpression("array(named_struct('transfer_type', 'from', 'account', 'another_account', 'product', product, 'subcode', subcode), named_struct('transfer_type', 'to', 'account', account, 'product', product, 'subcode', subcode))"))),
            (ExpressionRule("product = 'eqotc'"), RunOnPassProcessor(1000, Id(1043,1),
              OutputExpression("array(subcode('fromWithField', 6000), account_row('to', 'other_account1'))")))
          )

          val rules =
            for { ((exp, processor), idOffset) <- expressionRules.zipWithIndex }
              yield Rule(Id(100 * idOffset, 1), exp, processor)

          val rsId = Id(1, 1)
          val theRuleSuiteEngine = RuleSuite(rsId, Seq(
            RuleSet(Id(50, 1), rules
            )))

          val ruleEngineFactoryT = ProcessFunctions.ruleEngineFactoryT[Row, Seq[NewPosting]](theRuleSuiteEngine, DataType.fromDDL(DDL),
            compile = inCodegen, forceMutable = forceMutable, forceVarCompilation = forceVarCompilation)

          val jdbcDqProcessor: Processor[ResultSet, RuleEngineResult[Seq[NewPosting]]] = helper.wrapResultSet(ruleEngineFactoryT).instance

          val res = mapToLazyList(resultSet = resultSet, jdbcProcessor = jdbcDqProcessor)
          //ruleResults.map(_.overallResult) shouldBe Seq(Passed, Passed, Passed, Failed, Passed, Failed)
          for(i <- 0 until 3) {
            res(i).result.isEmpty shouldBe true
            res(i).salientRule.isEmpty shouldBe true
            res(i).ruleSuiteResults.overallResult shouldBe Failed
          }
          for(i <- 3 until 6) {
            res(i).result.isDefined shouldBe true
            res(i).salientRule.isDefined shouldBe true
            res(i).ruleSuiteResults.overallResult shouldBe Failed
          }

          res(3).result shouldBe Some(Seq(NewPosting("from", "another_account", "fx", 60), NewPosting("to","4206", "fx", 60)))
          res(3).salientRule shouldBe Some(SalientRule(Id(1,1),Id(50,1),Id(100,1)))

          res(4).result shouldBe Some(Seq(NewPosting("from", "another_account", "fxotc", 40), NewPosting("to","4201", "fxotc", 40)))
          res(4).salientRule shouldBe Some(SalientRule(Id(1,1),Id(50,1),Id(100,1)))

          res(5).result shouldBe Some(Seq(NewPosting("fromWithField", "4200", "eqotc", 6000), NewPosting("to","other_account1", "eqotc", 60)))
          res(5).salientRule shouldBe Some(SalientRule(Id(1,1),Id(50,1),Id(200,1)))
        }
      }
    }
  }

  private def mapToList[T](resultSet: ResultSet, jdbcProcessor: ResultSet => T): List[T] = {
    @tailrec
    def mapInner(listAcc: List[T]): List[T] = {
      val hasNext = resultSet.next()
      if (hasNext) {
        val next = jdbcProcessor(resultSet)
        mapInner(next :: listAcc)
      }
      else listAcc.reverse
    }

    mapInner(listAcc = Nil)
  }

  private def mapToLazyList[T](resultSet: ResultSet, jdbcProcessor: ResultSet => T): LazyList[T] = {
    val hasNext = resultSet.next()

    if (hasNext) jdbcProcessor(resultSet) #:: mapToLazyList(resultSet, jdbcProcessor)
    else LazyList.empty
  }


  override protected def beforeAll(): Unit = {
    val s = sparkSession // force it
    registerQualityFunctions()

    val properties = new Properties()

    properties.setProperty("user", "testUser")
    properties.setProperty("password", "testPass")

    connection = DriverManager.getConnection(urlString, properties)

    connection.prepareStatement("create schema test").executeUpdate()
    connection.prepareStatement("create table test.test_on (product TEXT(32) NOT NULL, account TEXT(32) NOT NULL, subcode INTEGER NOT NULL)").executeUpdate()

    testData.foreach { t =>
      connection.prepareStatement(s"insert into test.test_on values ('${t.product}', '${t.account}', ${t.subcode})").executeUpdate()
    }
    connection.commit()
  }


  override protected def afterAll(): Unit = {
    connection.close()
  }

}
