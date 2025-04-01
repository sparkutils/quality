package com.sparkutils.qualityTests

import com.sparkutils.quality._
import com.sparkutils.quality.impl.VersionedId
import com.sparkutils.quality.impl.extension.FunNRewrite
import org.apache.spark.sql.{DataFrame, QualitySparkUtils}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.qualityFunctions.PlaceHolderExpression
import org.apache.spark.sql.types.LongType
import org.junit.Test
import org.scalatest.FunSuite
import com.sparkutils.qualityTests.mapLookup.TradeTests._
import org.apache.spark.sql.ShimUtils.expression

class UserLambdaFunctionTest extends FunSuite with TestUtils {
  @Test
  def nullInParam: Unit = evalCodeGensNoResolve { funNRewrites {
    val blowup = "0"
    val funs = Seq(
      /*     LambdaFunction("group_account", "ACCOUNT_GROUP -> substr(ACCOUNT_GROUP,1,5)", Id(2,1)),
     LambdaFunction("fiscal_period3", "VALUE_DATE -> date_format(VALUE_DATE,'YYYYMM')", Id(3,1)),
     LambdaFunction("transaction_amount", "(DEBIT_OR_CREDIT_IDENTIFIER,TRANSACTION_CURRENCY_AMOUNT) -> case when DEBIT_OR_CREDIT_IDENTIFIER = 'D'  then TRANSACTION_CURRENCY_AMOUNT else TRANSACTION_CURRENCY_AMOUNT*-1 end", Id(4,2)),
 */
      LambdaFunction("posting_string_to_date",
        """(RULE_NO,TRANSACTION_CURRENCY_AMOUNT) -> named_struct('RULE_NO', RULE_NO,'TRANSACTION_AMOUNT', CAST(TRANSACTION_CURRENCY_AMOUNT AS DECIMAL(38,18)))""".stripMargin, Id(5, 2)),
      LambdaFunction("simple_bp_posting", "(RULE_NO,TRANSACTION_CURRENCY_AMOUNT) -> posting_string_to_date(RULE_NO, null)", Id(6, 2)),
      //LambdaFunction("simple_derivatives_posting","(RULE_NO,DEBIT_OR_CREDIT_IDENTIFIER,GENERAL_LEDGER_ACCOUNT_IDENTIFIER,POSTING_DATE,POSTING_TYPE_INDICATOR,TRANSACTION_CURRENCY_CODE) -> posting_string_to_date(RULE_NO,PROFIT_CENTER,ACCOUNTING_TRANSACTION_GROUP_IDENTIFIER,BUSINESS_DATE,BUSINESS_OBJECTIVE_IDENTIFIER,CLIENT_ADVISOR_IDENTIFIER,'','','',COMPLEX_CONTRACT_IDENTIFIER,COMPLEX_CONTRACT_SOURCE_SYSTEM_IDENTIFIER,COUNTRY_OF_INCORPORATION,current_timestamp(),DEBIT_OR_CREDIT_IDENTIFIER,DELIVERING_APPLICATION_SYSTEM_IDENTIFIER,'','',0,'',ELEMENTARY_CONTRACT_IDENTIFIER,ELEMENTARY_CONTRACT_SOURCE_SYSTEM_IDENTIFIER,FEED_RUN_IDENTIFIER,'',fiscal_period(TO_DATE(POSTING_DATE,'yyyyMMdd')),'','001',GENERAL_LEDGER_ACCOUNT_IDENTIFIER,group_account(GENERAL_LEDGER_ACCOUNT_IDENTIFIER),0,0,'','',INSTRUMENT_IDENTIFIER,INSTRUMENT_SOURCE_SYSTEM_IDENTIFIER,LEDGER_IDENTIFIER,'','','',PARTNER_RELATIONSHIP_IDENTIFIER,PARTNER_RELATIONSHIP_SOURCE_SYSTEM_IDENTIFIER,POSITION_IDENTIFIER,POSITION_SOURCE_SYSTEM_IDENTIFIER,POSTING_DATE,concat(AMOUNT_TYPE_DOMAIN,'_',AMOUNT_TYPE,'_',RULE_NO),POSTING_TEXT,POSTING_TYPE_INDICATOR,REGULATORY_PRODUCT_TYPE_CODE,REPORTING_AMOUNT_TYPE_CODE,RISK_HOLDING_LEGAL_ENTITY_IDENTIFIER,FEED_RUN_RECORD_SEQUENCE_NUMBER,'','','','',SUB_PRODUCT_TYPE_SUFFIX,ASSET_LIABILITY_TYPE,BOOK_IDENTIFIER,TRADING_PARTNER,transaction_amount(DEBIT_OR_CREDIT_IDENTIFIER,TRANSACTION_CURRENCY_AMOUNT),TRANSACTION_CURRENCY_CODE,TRANSACTION_IDENTIFIER,'' ,'' ,TRANSACTION_ORIGINATING_APPLICATION_SYSTEM_ID,TRANSACTION_ORIGINATING_APPLICATION_SYSTEM_ID,POSTING_DATE)", Id(7,2)),
      //LambdaFunction("simple_securitized_posting",s"(RULE_NO,DEBIT_OR_CREDIT_IDENTIFIER,FEED_SOURCE_SYSTEM_IDENTIFIER,GENERAL_LEDGER_ACCOUNT_IDENTIFIER,POSTING_DATE,POSTING_TYPE_INDICATOR,TRANSACTION_CURRENCY_CODE,TRANSACTION_LINK_IDENTIFIER,TRANSACTION_LINK_SOURCE,TRANSLATION_DATE) -> posting_string_to_date(RULE_NO,PROFIT_CENTER,ACCOUNTING_TRANSACTION_GROUP_IDENTIFIER,BUSINESS_DATE,BUSINESS_OBJECTIVE_IDENTIFIER,CLIENT_ADVISOR_IDENTIFIER,'','','',COMPLEX_CONTRACT_IDENTIFIER,COMPLEX_CONTRACT_SOURCE_SYSTEM_IDENTIFIER,COUNTRY_OF_INCORPORATION,current_timestamp(),DEBIT_OR_CREDIT_IDENTIFIER,DELIVERING_APPLICATION_SYSTEM_IDENTIFIER,'','',0,'',ELEMENTARY_CONTRACT_IDENTIFIER,ELEMENTARY_CONTRACT_SOURCE_SYSTEM_IDENTIFIER,FEED_RUN_IDENTIFIER,FEED_SOURCE_SYSTEM_IDENTIFIER,fiscal_period(TO_DATE(POSTING_DATE,'yyyyMMdd')),'','001',GENERAL_LEDGER_ACCOUNT_IDENTIFIER,group_account(GENERAL_LEDGER_ACCOUNT_IDENTIFIER),$blowup,0,'','',INSTRUMENT_IDENTIFIER,INSTRUMENT_SOURCE_SYSTEM_IDENTIFIER,LEDGER_IDENTIFIER,0,'','',PARTNER_RELATIONSHIP_IDENTIFIER,PARTNER_RELATIONSHIP_SOURCE_SYSTEM_IDENTIFIER,POSITION_IDENTIFIER,POSITION_SOURCE_SYSTEM_IDENTIFIER,POSTING_DATE,concat(AMOUNT_TYPE_DOMAIN,'_',AMOUNT_TYPE,'_',RULE_NO),POSTING_TEXT,POSTING_TYPE_INDICATOR,REGULATORY_PRODUCT_TYPE_CODE,REPORTING_AMOUNT_TYPE_CODE,RISK_HOLDING_LEGAL_ENTITY_IDENTIFIER,FEED_RUN_RECORD_SEQUENCE_NUMBER,0,0,'','',SUB_PRODUCT_TYPE_SUFFIX,ASSET_LIABILITY_TYPE,BOOK_IDENTIFIER,TRADING_PARTNER,transaction_amount(DEBIT_OR_CREDIT_IDENTIFIER,TRANSACTION_CURRENCY_AMOUNT),TRANSACTION_CURRENCY_CODE,TRANSACTION_IDENTIFIER,TRANSACTION_LINK_IDENTIFIER,TRANSACTION_LINK_SOURCE,TRANSACTION_ORIGINATING_APPLICATION_SYSTEM_ID,TRANSACTION_ORIGINATING_APPLICATION_SYSTEM_ID,TRANSLATION_DATE)", Id(8,2)),

      LambdaFunction("multValCCY", "(theValue, ccy) -> theValue * decimal(ccy)", Id(9, 2)),
      LambdaFunction("q1", "(theValue, ccy) -> multValCCY(theValue, ccy)", Id(10, 2))
    )
    registerLambdaFunctions(funs)

    val ndf =
      sparkSession.range(1).selectExpr("simple_bp_posting('a_rule', null) as test")
    //"named_struct('e', decimal(null), 'f', 1) as test")//" q1(1, null) as test")//simple_bp_posting('a_rule', null) as test")

    ndf.head

  } }

  @Test
  def lambdaRuleTest: Unit = evalCodeGensNoResolve { funNRewrites {

    import sparkSession.implicits._
    val df = simpleTrades.toDF(tradeCols :_ *)

    //val rule = LambdaFunction("multValCCY", "(theValue: BIGINT, ccy: DECIMAL) -> theValue * ccy", Id(1,2))
    val rule = LambdaFunction("multValCCY", "(theValue, ccy) -> theValue * ccy", Id(1,2))
    registerLambdaFunctions(Seq(rule))

    val ndf = df.withColumn("newcalc", expr("multValCCY(value, ccyrate)"))

    doTest(ndf)
  } }

  @Test
  def lambdaNoParamsRuleTest: Unit = evalCodeGensNoResolve { funNRewrites {

    import sparkSession.implicits._
    val df = simpleTrades.toDF(tradeCols :_ *)

    //val rule = LambdaFunction("multValCCY", "(theValue: BIGINT, ccy: DECIMAL) -> theValue * ccy", Id(1,2))
    val rule = LambdaFunction("multValCCY", "value * ccyrate", Id(1,2))
    registerLambdaFunctions(Seq(rule))

    val ndf = df.withColumn("newcalc", expr("multValCCY()"))

    doTest(ndf)
  } }

  def doTest(ndf: DataFrame): Unit = {
    debug{ndf.show()}
    ndf.collect().foreach{r => assert(r.getAs[Double]("newcalc") == r.getAs[Int]("value") * r.getAs[Double]("ccyrate")) }
  }

  @Test
  def lambdaMultiParamLengthExpandedTest: Unit = evalCodeGensNoResolve { funNRewrites {

    import sparkSession.implicits._
    val df = simpleTrades.toDF(tradeCols :_ *)

    //val rule = LambdaFunction("multValCCY", "(theValue: BIGINT, ccy: DECIMAL) -> theValue * ccy", Id(1,2))
    val rule = LambdaFunction("multValCCY", "value * ccyrate", Id(1,2))
    val rule1 = LambdaFunction("multValCCY", "theValue -> theValue * ccyrate", Id(2,2))
    val rule2 = LambdaFunction("multValCCY", "(theValue, ccy) -> theValue * ccy", Id(3,2))
    registerLambdaFunctions(Seq(rule, rule1, rule2))

    // all of these should work
    doTest(df.withColumn("newcalc", expr("multValCCY()")))
    doTest(df.withColumn("newcalc", expr("multValCCY(value)")))
    doTest(df.withColumn("newcalc", expr("multValCCY(value, ccyrate)")))
  } }

  @Test
  def lambdaMultiParamLengthSelfReferenceTest: Unit = evalCodeGensNoResolve { funNRewrites {

    import sparkSession.implicits._
    val df = simpleTrades.toDF(tradeCols :_ *)

    //val rule = LambdaFunction("multValCCY", "(theValue: BIGINT, ccy: DECIMAL) -> theValue * ccy", Id(1,2))
    val rule = LambdaFunction("multValCCY", "multValCCY(value, ccyrate)", Id(1,2))
    val rule1 = LambdaFunction("multValCCY", "theValue -> multValCCY(theValue, ccyrate)", Id(2,2))
    val rule2 = LambdaFunction("multValCCY", "(theValue, ccy) -> theValue * ccy", Id(3,2))
    registerLambdaFunctions(Seq(rule, rule1, rule2))

    // all of these should work
    doTest(df.withColumn("newcalc", expr("multValCCY()")))
    doTest(df.withColumn("newcalc", expr("multValCCY(value)")))
    doTest(df.withColumn("newcalc", expr("multValCCY(value, ccyrate)")))
  } }

  @Test
  def lambdaMultiParamDupeLengthTest: Unit = evalCodeGensNoResolve { funNRewrites {

    import sparkSession.implicits._
    val df = simpleTrades.toDF(tradeCols :_ *)

    //val rule = LambdaFunction("multValCCY", "(theValue: BIGINT, ccy: DECIMAL) -> theValue * ccy", Id(1,2))
    val rule = LambdaFunction("multValCCY", "multValCCY(value, ccyrate)", Id(1,2))
    val rule1 = LambdaFunction("multValCCY", "(theValue, ccy) -> theValue * ccy", Id(2,2))
    val rule2 = LambdaFunction("multValCCY", "(theValue, ccy) -> theValue * ccy", Id(3,2))
    try {
      registerLambdaFunctions(Seq(rule, rule1, rule2))
    } catch {
      case e: Exception => assert(e.getMessage.contains("Lambda function multValCCY has 2 implementations with 2 arguments: "))
    }
  } }

  @Test
  def lambdaMissing0LengthTest: Unit = evalCodeGensNoResolve { funNRewrites {

    import sparkSession.implicits._
    val df = simpleTrades.toDF(tradeCols :_ *)

    //val rule = LambdaFunction("multValCCY", "(theValue: BIGINT, ccy: DECIMAL) -> theValue * ccy", Id(1,2))
    val rule = LambdaFunction("multValCCY", "(theValue, ccy) -> theValue * ccy", Id(3,2))
    registerLambdaFunctions(Seq(rule))

    try {
      doTest(df.withColumn("newcalc", expr("multValCCY()")))
    } catch {
      case e: Exception => assert(e.getMessage.contains("0 arguments requested for multValCCY but no implementation with this argument count exists"))
    }
  } }

  @Test
  def nestedLambdaRuleTest: Unit = evalCodeGensNoResolve { funNRewrites {

    import sparkSession.implicits._
    val df = simpleTrades.toDF(tradeCols :_ *)

    //val rule = LambdaFunction("multValCCY", "(theValue: BIGINT, ccy: DECIMAL) -> theValue * ccy", Id(1,2))
    val rule = LambdaFunction("multValCCY", "(theValue, ccy) -> theValue * ccy", Id(1,2))
    val rule2 = LambdaFunction("multThenAddWhenCHFOrValue", "(aValue, ccy, rate) -> IF(ccy='CHF', multValCCY(aValue, rate) + aValue, aValue)", Id(2,2))
    registerLambdaFunctions(Seq(rule, rule2))

    val ndf = df.withColumn("newcalc", expr("multThenAddWhenCHFOrValue(value, ccy, ccyrate)"))

    debug(ndf.show())
    ndf.collect().foreach{r => assert( r.getAs[Double]("newcalc") == (if (r.getAs[String]("ccy") == "CHF")
        (r.getAs[Int]("value") * r.getAs[Double]("ccyrate")) + r.getAs[Int]("value")
      else
        r.getAs[Int]("value")
      )
    ) }
  } }

  @Test
  def globalLambdasTest: Unit = { funNRewrites {
    val rs = Map( (Id(0,1): VersionedId )-> RuleSuite(Id(0,1), Seq(RuleSet(Id(1,1), Seq(
      Rule(Id(2,1), ExpressionRule("fielda > fieldb"), RunOnPassProcessor(0, Id(100,1), OutputExpression("fielda > b"))),
      Rule(Id(3,1), ExpressionRule("fielda > fieldb"), RunOnPassProcessor(0, Id(100,1), OutputExpression("fielda >> fieldb"))),
      Rule(Id(4,1), ExpressionRule("fielda > b")),
      Rule(Id(5,1), ExpressionRule("fielda >> fieldb"))
    ))) ) )

    def lambdas(global: Id = Id(-1,-1)) = Map( Id(0,1) -> Seq(LambdaFunction("test", "variable -> outervariable", Id(6,1)),
      LambdaFunction("testCaller", "outervariable -> test(outervariable)", Id(7,2)),
      LambdaFunction("testCaller2", "(outervariable) -> test(outervariable)", Id(8,1))),
      global -> Seq(LambdaFunction("I_AM_GLOBAL", "variable -> outervariable", Id(16,1)))
    )

    val without = integrateLambdas(rs, lambdas()).head
    assert(!without._2.lambdaFunctions.exists(l => l.name == "I_AM_GLOBAL"))

    val withGlobal = integrateLambdas(rs, lambdas(), Some(Id(-1,-1))).head
    assert(withGlobal._2.lambdaFunctions.exists(l => l.name == "I_AM_GLOBAL"))

    val withGlobalThatDoesntExist = integrateLambdas(rs, lambdas(Id(-2,-2)), Some(Id(-1,-1))).head
    assert(!withGlobalThatDoesntExist._2.lambdaFunctions.exists(l => l.name == "I_AM_GLOBAL"))

  } }

  /**
   * test's functions as params to lambdas, partial application cases are also
   * tested in the AggregatesTest - impl needs interpreted as the type of FunForward really isn't long
   */
  @Test
  def hofTest: Unit = evalCodeGensNoResolve { funNRewrites {
    val mult = LambdaFunction("mult", "(theValue, ccy) -> theValue * ccy", Id(1,2))
    val plus = LambdaFunction("plus", "(theValue, ccy) -> theValue + ccy", Id(3,2))
    val user = LambdaFunction("use", "(func, a, b) -> callFun(func, a, b)", Id(2,2))
    val use1 = LambdaFunction("use1", "(func, b) -> callFun(func, b)", Id(4,2))
    val deep = LambdaFunction("deep", "(func, a, b) -> use(func, a, b)", Id(2,2))
    val deep1 = LambdaFunction("deep1", "(func, b) -> use1(func, b)", Id(2,2))
    registerLambdaFunctions(Seq(mult, plus, user, use1, deep, deep1))

    import sparkSession.implicits._

    // types per default placeholder of LongType
    assert(2L == sparkSession.sql("select use(mult(_(), _()), 1L, 2L) as res").as[Long].head)
    assert(3L == sparkSession.sql("select use(plus(_(), _()), 1L, 2L) as res").as[Long].head)

    // specify types in placeholder
    assert(2 == sparkSession.sql("select use(mult(_('int'), _('int')), 1, 2) as res").as[Int].head)
    assert(3 == sparkSession.sql("select use(plus(_('int'), _('int')), 1, 2) as res").as[Int].head)

    // partially apply 1st arg
    assert(2L == sparkSession.sql("select use1(mult(1L, _()), 2L) as res").as[Long].head)
    assert(3L == sparkSession.sql("select use1(plus(1L, _()), 2L) as res").as[Long].head)

    // partially apply 2nd arg
    assert(2L == {
      val df = sparkSession.sql("select use1(mult(_(), 2L), 1L) as res")
      df
    }.as[Long].head)
    assert(3L == sparkSession.sql("select use1(plus(_(), 2L), 1L) as res").as[Long].head)

    // nested
    assert(2 == sparkSession.sql("select deep(mult(_('int'), _('int')), 1, 2) as res").as[Int].head)
    assert(3 == sparkSession.sql("select deep(plus(_('int'), _('int')), 1, 2) as res").as[Int].head)

    // nested partial
    assert(2 == sparkSession.sql("select deep1(mult(1, _('int')), 2) as res").as[Int].head)
    assert(3 == sparkSession.sql("select deep1(plus(1, _('int')), 2) as res").as[Int].head)
  } }

  @Test
  def deepPartialTest: Unit = evalCodeGensNoResolve { funNRewrites {
    val plus2 = LambdaFunction("plus", "(a, b) -> a + b", Id(3,2))
    val plus3 = LambdaFunction("plus", "(a, b, c) -> plus(plus(a, b), c)", Id(3,2))
    val papplyt = LambdaFunction("papplyt", "(func, a, b, c) -> callFun(callFun(func, _(), _(), c), a, b)", Id(2,2))
    registerLambdaFunctions(Seq(plus2, plus3, papplyt))

    import sparkSession.implicits._

    assert(6L == { val sql = sparkSession.sql("select papplyt(plus(_(), _(), _()), 1L, 2L, 3L) as res")
      sql.as[Long].head})
  } }

  @Test
  def returnLambdaTest: Unit = evalCodeGensNoResolve { funNRewrites {
    val plus2 = LambdaFunction("plus", "(a, b) -> a + b", Id(3,2))
    val plus3 = LambdaFunction("plus", "(a, b, c) -> plus(plus(a, b), c)", Id(3,2))
    val retLambda = LambdaFunction("retLambda", "(a, b) -> plus(a, b, _())", Id(2,2))
    registerLambdaFunctions(Seq(plus2, plus3, retLambda))

    import sparkSession.implicits._

    assert(6L == { val sql = sparkSession.sql("select callFun(retLambda(1L, 2L), 3L) as res")
      sql.as[Long].head})
  } }

  def doHOFLambdaDropin() = {
    val plus = LambdaFunction("plus", "(theValue, ccy) -> theValue + ccy", Id(1,2))
    val plus3 = LambdaFunction("plus3", "(theValue, ccy, c) -> theValue + ccy + c", Id(2,2))
    val hof = LambdaFunction("hof", "func -> aggregate(array(1, 2, 3), 0, _lambda_(func))", Id(3,2))
    registerLambdaFunctions(Seq(plus, plus3, hof))

    import sparkSession.implicits._

    // attempt to dropping a reference to a function where simple lambdas are expected.
    // control
    assert(6 == sparkSession.sql("SELECT aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x) as res").as[Int].head)
    // all params would be needed with multiple aritys
    assert(6 == sparkSession.sql("SELECT aggregate(array(1, 2, 3), 0, _lambda_(plus(_('int'), _('int')))) as res").as[Int].head)
    // can we play with partials?
    assert(21 == sparkSession.sql("SELECT aggregate(array(1, 2, 3), 0, _lambda_(plus3(_('int'), _('int'), 5))) as res").as[Int].head)
    // hof'd
    assert(6 == sparkSession.sql("SELECT hof(plus(_('int'), _('int'))) as res").as[Int].head)
  }

  @Test
  def testHOFLambdaDropin(): Unit = evalCodeGensNoResolve {  funNRewrites { doHOFLambdaDropin() } }

  /**
   * Although Dropin tests aggregate and combinations to prove overall behaviour, in light of the issue detected in DBRs
   * this tests all the other in built hofs.  Examples taken from the usage guide annotations
   */
  @Test
  def testHOFDropins(): Unit = evalCodeGensNoResolve { funNRewrites {
    val plus = LambdaFunction("plus", "(a, b) -> a + b", Id(1, 2))
    val times = LambdaFunction("times", "(a, b) -> a * b", Id(2, 2))
    val sort1 = LambdaFunction("sort1", "(lefty, righty) -> case when lefty < righty then -1 when lefty > righty then 1 else 0 end", Id(3, 2))
    val sort2 = LambdaFunction("sort2", "(lefty, righty) -> case when lefty is null and righty is null then 0 when lefty is null then -1 when righty is null then 1 when lefty < righty then 1 when lefty > righty then -1 else 0 end", Id(3, 2))
    val gt = LambdaFunction("gt", "(a, b) -> a > b", Id(2, 2))
    val ismod = LambdaFunction("ismod", "(a, b, c) -> a % b == c", Id(2, 2))
    val notnull = LambdaFunction("notnull", "x -> x IS NOT NULL", Id(2, 2))
    val isnull = LambdaFunction("isnull", "x -> x IS NULL", Id(2, 2))
    val drop2nd = LambdaFunction("drop2nd", "(f, x, y) -> callFun(f, x)", Id(2, 2))
    val dropNConcat = LambdaFunction("dropNConcat", "(k, v1, v2) -> concat(v1, v2)", Id(2, 2))
    registerLambdaFunctions(Seq(plus, times, sort1, sort2, gt, ismod, notnull, isnull, drop2nd, dropNConcat))

    import sparkSession.implicits._
    // SELECT _FUNC_(array(1, 2, 3), 0, (acc, x) -> acc + x, acc -> acc * 10);
    assert(60 == sparkSession.sql("SELECT aggregate(array(1, 2, 3), 0, _lambda_(plus(_('int'), _('int'))), _lambda_(times(_('int'), 10))) as res").as[Int].head)
    // SELECT _FUNC_(array(1, 2, 3), x -> x + 1);
    assert(Seq(2, 3, 4) == sparkSession.sql("SELECT transform(array(1, 2, 3), _lambda_(plus(_('int'), 1))) as res").as[Seq[Int]].head)
    //> SELECT _FUNC_(array(1, 2, 3), (x, i) -> x + i);
    assert(Seq(1, 3, 5) == sparkSession.sql("SELECT transform(array(1, 2, 3), _lambda_(plus(_('int'), _('int')))) as res").as[Seq[Int]].head)

    not2_4 {
      //> SELECT _FUNC_(array(5, 6, 1), (left, right) -> case when left < right then -1 when left > right then 1 else 0 end);
      //[1,5,6]
      assert(Seq(1, 5, 6) == sparkSession.sql("SELECT array_sort(array(5, 6, 1), _lambda_(sort1(_('int'), _('int')))) as res").as[Seq[Int]].head)
      //> SELECT _FUNC_(array('bc', 'ab', 'dc'), (left, right) -> case when left is null and right is null then 0 when left is null then -1 when right is null then 1 when left < right then 1 when left > right then -1 else 0 end);
      //["dc","bc","ab"]
      assert(Seq("dc", "bc", "ab") == sparkSession.sql("SELECT array_sort(array('bc', 'ab', 'dc'), _lambda_(sort2(_('string'), _('string')))) as res").as[Seq[String]].head)
    }

    not2_4 {
      //> SELECT _FUNC_(map(1, 0, 2, 2, 3, -1), (k, v) -> k > v);
      //       {1:0,3:-1}
      assert(Map(1 -> 0, 3 -> -1) == sparkSession.sql("SELECT map_filter(map(1, 0, 2, 2, 3, -1), _lambda_(gt(_('int'), _('int')))) as res").as[Map[Int, Int]].head)
    }

    //> SELECT _FUNC_(array(1, 2, 3), x -> x % 2 == 1);
    //       [1,3]
    //      > SELECT _FUNC_(array(0, 2, 3), (x, i) -> x > i);
    //       [2,3]
    //      > SELECT _FUNC_(array(0, null, 2, 3, null), x -> x IS NOT NULL);
    //       [0,2,3]
    assert(Seq(1, 3) == sparkSession.sql("SELECT filter(array(1, 2, 3), _lambda_(ismod(_('int'), 2, 1))) as res").as[Seq[Int]].head)
    not2_4 {
      assert(Seq(2, 3) == sparkSession.sql("SELECT filter(array(0, 2, 3), _lambda_(gt(_('int'), _('int')))) as res").as[Seq[Int]].head)
    }
    assert(Seq(0, 2, 3) == sparkSession.sql("SELECT filter(array(0, null, 2, 3, null), _lambda_(notnull(_('int')))) as res").as[Seq[Int]].head)

    // > SELECT _FUNC_(array(1, 2, 3), x -> x % 2 == 0);
    //       true
    //      > SELECT _FUNC_(array(1, 2, 3), x -> x % 2 == 10);
    //       false
    //      > SELECT _FUNC_(array(1, null, 3), x -> x % 2 == 0);
    //       NULL
    //      > SELECT _FUNC_(array(0, null, 2, 3, null), x -> x IS NULL);
    //       true
    //      > SELECT _FUNC_(array(1, 2, 3), x -> x IS NULL);
    //       false
    assert(sparkSession.sql("SELECT exists(array(1, 2, 3), _lambda_(ismod(_('int'), 2, 0))) as res").as[Boolean].head)
    assert(!sparkSession.sql("SELECT exists(array(1, 2, 3), _lambda_(ismod(_('int'), 2, 10))) as res").as[Boolean].head)
    not2_4 {
      assert(sparkSession.sql("SELECT exists(array(1, null, 3), _lambda_(ismod(_('int'), 2, 0))) as res").head.isNullAt(0))
      assert(sparkSession.sql("SELECT exists(array(0, null, 2, 3, null), _lambda_(isnull(_('int')))) as res").as[Boolean].head)
    }
    only2_4 {
      assert(!sparkSession.sql("SELECT exists(array(1, null, 3), _lambda_(ismod(_('int'), 2, 0))) as res").as[Boolean].head)
      assert(sparkSession.sql("SELECT exists(array(0, null, 2, 3, null), _lambda_(isnull(_('int')))) as res").as[Boolean].head)
    }
    assert(!sparkSession.sql("SELECT exists(array(1, 2, 3), _lambda_(isnull(_('int')))) as res").as[Boolean].head)

    not2_4 {
      //> SELECT _FUNC_(array(1, null, 3), x -> x % 2 == 0);
      //       false
      //      > SELECT _FUNC_(array(2, null, 8), x -> x % 2 == 0);
      //       NULL
      assert(!sparkSession.sql("SELECT forall(array(1, null, 3), _lambda_(ismod(_('int'), 2, 0))) as res").as[Boolean].head)
      assert(sparkSession.sql("SELECT forall(array(2, null, 8), _lambda_(ismod(_('int'), 2, 0))) as res").head.isNullAt(0))
    }

    not2_4_or_3_0_or_3_1 {

      //  3.0 / 3.1 for transform_keys and transform_values
      //@transient lazy val LambdaFunction(
      //_, (keyVar: NamedLambdaVariable) :: (valueVar: NamedLambdaVariable) :: Nil, _) = function
      // but map_zip_with is
      //   @transient lazy val LambdaFunction(_, Seq(
      //    keyVar: NamedLambdaVariable,
      //    value1Var: NamedLambdaVariable,
      //    value2Var: NamedLambdaVariable),
      //    _) = function
      // and the actual is:
      // lambdafunction(plus(lambda x#146, 1, lambdafunction((lambda a#144 + lambda b#145), lambda a#144, lambda b#145, false), Some(plus), false, false), lambda x#146, lambda y#147, false)
      // in 3.3.0 it's
      //   @transient lazy val LambdaFunction(
      //    _, Seq(keyVar: NamedLambdaVariable, valueVar: NamedLambdaVariable), _) = function
      // so it only seems to match for List and not Seq, which is oddly specific

      // > SELECT _FUNC_(map_from_arrays(array(1, 2, 3), array(1, 2, 3)), (k, v) -> k + 1);
      //       {2:1,3:2,4:3}
      //      > SELECT _FUNC_(map_from_arrays(array(1, 2, 3), array(1, 2, 3)), (k, v) -> k + v);
      //       {2:1,4:2,6:3}
      //transform_keys
      assert(Map(2 -> 1, 3 -> 2, 4 -> 3) == sparkSession.sql("SELECT transform_keys(map_from_arrays(array(1, 2, 3), array(1, 2, 3)), _lambda_(drop2nd(plus(_('int'), 1),_('int'), _('int')))) as res").as[Map[Int, Int]].head)
      assert(Map(2 -> 1, 4 -> 2, 6 -> 3) == sparkSession.sql("SELECT transform_keys(map_from_arrays(array(1, 2, 3), array(1, 2, 3)), _lambda_(plus(_('int'), _('int')))) as res").as[Map[Int, Int]].head)

      // > SELECT _FUNC_(map_from_arrays(array(1, 2, 3), array(1, 2, 3)), (k, v) -> v + 1);
      //       {1:2,2:3,3:4}
      //      > SELECT _FUNC_(map_from_arrays(array(1, 2, 3), array(1, 2, 3)), (k, v) -> k + v);
      //       {1:2,2:4,3:6}
      assert(Map(1 -> 2, 2 -> 3, 3 -> 4) == sparkSession.sql("SELECT transform_values(map_from_arrays(array(1, 2, 3), array(1, 2, 3)), _lambda_(drop2nd(plus(_('int'), 1),_('int'), _('int')))) as res").as[Map[Int, Int]].head)
      assert(Map(1 -> 2, 2 -> 4, 3 -> 6) == sparkSession.sql("SELECT transform_values(map_from_arrays(array(1, 2, 3), array(1, 2, 3)), _lambda_(plus(_('int'), _('int')))) as res").as[Map[Int, Int]].head)
    }

    not2_4 {
      // > SELECT _FUNC_(map(1, 'a', 2, 'b'), map(1, 'x', 2, 'y'), (k, v1, v2) -> concat(v1, v2));
      //       {1:"ax",2:"by"}
      // map_zip_with
      assert(Map(1 -> "ax", 2 -> "by") == sparkSession.sql("SELECT map_zip_with(map(1, 'a', 2, 'b'), map(1, 'x', 2, 'y'), _lambda_(dropNConcat(_('int'), _('string'), _('string')))) as res").as[Map[Int, String]].head)
    }


  //> SELECT _FUNC_(array(1, 2), array(3, 4), (x, y) -> x + y);
    //       [4,6]
    assert(Seq(4,6) == sparkSession.sql("SELECT zip_With(array(1, 2), array(3, 4), _lambda_(plus(_('int'), _('int')))) as res").as[Seq[Int]].head)
  } }

  @Test
  def testHOFFunForwardDropin(): Unit = evalCodeGensNoResolve { funNRewrites {
    val plus = LambdaFunction("plus", "(a, b, c) -> a + b", Id(1, 2))
    val funF = LambdaFunction("funf", "f -> aggregate(array(1, 2, 3), 0, _lambda_(f) )", Id(1, 2)) // ignore the params but keep the shape
    registerLambdaFunctions(Seq(plus, funF))

    import sparkSession.implicits._
    assert(9 == sparkSession.sql("SELECT funf(plus(_('int'), 3, _('int'))) as res").as[Int].head)
  } }

  @Test
  def testPlaceHolderNullableOverrides(): Unit = evalCodeGensNoResolve { funNRewrites {
    val resolve = SparkTestUtils.resolveBuiltinOrTempFunction(sparkSession) _
    // as these cannot be tested as part of runtimes with aggregate bug resolve is used to directly test
    val actualDefaultCall = resolve("_", Seq(expression(lit("int")))).get
    assert(actualDefaultCall.nullable)
    val actualOverriddenCall = resolve("_", Seq(expression(lit("int")), expression(lit(false)))).get
    assert(!actualOverriddenCall.nullable)

    val plus = LambdaFunction("plus", "(a, b) -> a + b", Id(1,2))
    // this isn't actually tested for false or true as the top level binding overrides it, but it's tested to prove coverage
    val test = LambdaFunction("plusTest", "(f, a) -> callFun(callFun(f, _('long', false), 1), a)", Id(3,2))
    val test2 = LambdaFunction("plusTest2", "(f, a) -> callFun(callFun(f, _('long'), 1), a)", Id(3,2))
    registerLambdaFunctions(Seq(plus, test, test2))

    var shouldBeNull = sparkSession.sql("select plusTest(plus(_(), _()), null)").head
    assert(shouldBeNull.isNullAt(0))
    shouldBeNull = sparkSession.sql("select plusTest(plus(_(), _('int', false)), null)").head
    assert(shouldBeNull.isNullAt(0))
    val control = sparkSession.sql("select plusTest(plus(_(), _()), 1L)").head
    assert(!control.isNullAt(0))
    assert(control.get(0) == 2)
    val control2 = sparkSession.sql("select plusTest2(plus(_(), _()), 1L)").head
    assert(!control2.isNullAt(0))
    assert(control2.get(0) == 2)
  } }

  @Test
  def testCallFunForward(): Unit = evalCodeGensNoResolve { funNRewrites {
    val plus = LambdaFunction("plus", "(a, b) -> a + b", Id(1,2))
    val opargs = LambdaFunction("opargs", "(op, a, b) -> callFun(op, a, b)", Id(1,2))
    // this is utter nonsense -
    val test = LambdaFunction("plusTest", "(f, a) -> callFun(callFun(f, _(), 1L), a)", Id(3,2))
    registerLambdaFunctions(Seq(plus, test, opargs))

    val control = sparkSession.sql("select plusTest(opargs(plus(_(), _()), _(), _()), 1L)").head
    assert(!control.isNullAt(0))
    assert(control.get(0) == 2)
  } }
  /*
  @Test
  def runLoadsEval(): Unit = forceInterpreted{ loadsOf{ doHOFDropin() }}
  @Test
  def runLoadsCodeGen(): Unit = forceCodeGen{ loadsOf{  doHOFDropin() }} */
}
