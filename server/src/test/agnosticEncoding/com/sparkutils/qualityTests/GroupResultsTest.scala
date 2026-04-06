package com.sparkutils.qualityTests

import com.sparkutils.quality._
import com.sparkutils.qualityTests.RuleEngineTest.{rulesRaw, testData}
import com.sparkutils.qualityTests.util.SharedPureConnectTests
import frameless.TypedEncoder
import org.scalatest.Matchers
import com.sparkutils.quality.implicits._

trait GroupTestBase extends SharedPureConnectTests with Matchers {

  def group = RuleSuiteGroup(
    Set(
      rulesRaw(
        Seq((ExpressionRule("product = 'edt' and subcode = 40"), RunOnPassProcessor(1000, Id(1040, 1),
          OutputExpression("array(account_row('from'), account_row('to', 'other_account1'))"))),
          (ExpressionRule("product like '%fx%'"), RunOnPassProcessor(1000, Id(1042, 1),
            OutputExpression("array(named_struct('transfer_type', 'from', 'account', 'another_account', 'product', product, 'subcode', subcode), named_struct('transfer_type', 'to', 'account', account, 'product', product, 'subcode', subcode))"))),
          (ExpressionRule("product = 'eqotc'"), RunOnPassProcessor(1000, Id(1043, 1),
            OutputExpression("array(subcode('fromWithField', 6000), account_row('to', 'other_account1'))")))
        )
      ).copy(id = Id(1,1)),
      rulesRaw(
        Seq((ExpressionRule("product = 'edt' and subcode = 40"), RunOnPassProcessor(1000, Id(1040, 1),
          OutputExpression("array(account_row('from'), account_row('to', 'other_account1'))"))),
          (ExpressionRule("product like '%fx%'"), RunOnPassProcessor(1000, Id(1042, 1),
            OutputExpression("array(named_struct('transfer_type', 'from', 'account', 'another_account', 'product', product, 'subcode', subcode), named_struct('transfer_type', 'to', 'account', account, 'product', product, 'subcode', subcode))"))),
          (ExpressionRule("product = 'eqotc'"), RunOnPassProcessor(1000, Id(1043, 1),
            OutputExpression("array(subcode('fromWithField', 6000), account_row('to', 'other_account1'))")))
        )
      ).copy(id = Id(2,1)),
      rulesRaw(
        Seq((ExpressionRule("product = 'edt' and subcode = 40"), RunOnPassProcessor(1000, Id(1040, 1),
          OutputExpression("array(account_row('from'), account_row('to', 'other_account1'))"))),
          (ExpressionRule("product like '%fx%'"), RunOnPassProcessor(1000, Id(1042, 1),
            OutputExpression("array(named_struct('transfer_type', 'from', 'account', 'another_account', 'product', product, 'subcode', subcode), named_struct('transfer_type', 'to', 'account', account, 'product', product, 'subcode', subcode))"))),
          (ExpressionRule("product = 'eqotc'"), RunOnPassProcessor(1000, Id(1043, 1),
            OutputExpression("array(subcode('fromWithField', 6000), account_row('to', 'other_account1'))")))
        )
      ).copy(id = Id(3,1))
    )
  )

  def verifyRuleSuites[T](r: Seq[T])(ruleSuiteGroup: T => RuleSuiteGroupResults): Unit = {
    r.map(ruleSuiteGroup(_).ruleSuiteResults.keys.toSeq).distinct.map(_.sortBy(i=>(i.id, i.version))) shouldBe Seq(
      Seq(Id(1,1), Id(2,1), Id(3,1))
    )
  }

  val group_function: String

  def dqResultsShouldGroup(arrayStart: String = "array(", arrayEnd: String = ")"): Unit = {
    val name = register_rule_suite_group(group)
    val s = sparkSession
    import s.implicits._
    val tds = testData.toDS()
    val ds = tds.selectExpr(s"$group_function( $arrayStart dq_rule_runner(rule_suite_from($name, 1)), " +
      s"dq_rule_runner(rule_suite_from($name, 2)), dq_rule_runner(rule_suite_from($name, 3)) $arrayEnd ) as res")
    //ds.show()
    import com.sparkutils.quality.implicits._
    val r = ds.selectExpr("res.*").as[RuleSuiteGroupResults].collect()
    verifyRuleSuites(r)(_)
  }

  def engineResultShouldGroup[T: TypedEncoder](conv: T => RuleSuiteGroupResults, arrayStart: String = "array(", arrayEnd: String = ")"): Unit = {
    val name = register_rule_suite_group(group)
    val s = sparkSession
    val tds = {
      import s.implicits._
      testData.toDS()
    }
    val ds = tds.selectExpr(s"$group_function( $arrayStart rule_engine_runner(rule_suite_from($name, 1)), " +
      s"rule_engine_runner(rule_suite_from($name, 2)), rule_engine_runner(rule_suite_from($name, 3)) $arrayEnd ) as res")
    //ds.show()

    import frameless._
    implicit val enc = TypedExpressionEncoder[T]

    val r = ds.selectExpr("res.*").as[T].collect()
    verifyRuleSuites(r)(conv)
  }

  def folderResultShouldGroup[T: TypedEncoder](conv: T => RuleSuiteGroupResults, arrayStart: String = "array(", arrayEnd: String = ")"): Unit = {
    // folder needs a struct / row, so wrap the array up in outputs, starter and in the type
    val name = register_rule_suite_group(group.copy(ruleSuites = group.ruleSuites.mapValues(v => RuleSuite.mapRules(v){
      r =>
        val nrop = r.runOnPassProcessor.withExpr( OutputExpression( "cur -> struct(" +
          r.runOnPassProcessor.outputExpression.asInstanceOf[HasRuleText].rule +" )"))
        r.copy( runOnPassProcessor = nrop )
    }).toMap))
    val s = sparkSession
    val tds = {
      import s.implicits._
      testData.toDS()
    }
    val starter = ", named_struct('arr', array(struct('' as transfer_type, account, product, subcode)))"

    val ds = tds.selectExpr(s"$group_function( $arrayStart rule_folder_runner(rule_suite_from($name, 1)$starter), " +
      s"rule_folder_runner(rule_suite_from($name, 2)$starter), rule_folder_runner(rule_suite_from($name, 3)$starter) $arrayEnd ) as res")
    //ds.show()

    import frameless._
    implicit val enc = TypedExpressionEncoder[T]
    // ds.printSchema()

    val r = ds.selectExpr("res.*").as[T].collect()
    verifyRuleSuites(r)(conv)
  }

  def collectorResultShouldGroup[T: TypedEncoder](conv: T => RuleSuiteGroupResults, arrayStart: String = "array(", arrayEnd: String = ")"): Unit = {

    val name = register_rule_suite_group(group)
    val s = sparkSession
    val tds = {
      import s.implicits._
      testData.toDS()
    }

    val ds = tds.selectExpr(s"$group_function( $arrayStart collect_runner(rule_suite_from($name, 1)), " +
      s"collect_runner(rule_suite_from($name, 2)), collect_runner(rule_suite_from($name, 3)) $arrayEnd ) as res")
    //ds.show()

    import frameless._
    implicit val enc = TypedExpressionEncoder[T]
    // ds.printSchema()

    val r = ds.selectExpr("res.*").as[T].collect()
    verifyRuleSuites(r)(conv)
  }

  def resultGroupsShouldGroup(arrayStart: String = "array(", arrayEnd: String = ")"): Unit = {
    val name = register_rule_suite_group(group)
    val s = sparkSession
    import s.implicits._
    val tds = testData.toDS()
    val sub = s"$group_function( $arrayStart dq_rule_runner(rule_suite_from($name, 1)), " +
      s"dq_rule_runner(rule_suite_from($name, 2)), dq_rule_runner(rule_suite_from($name, 3)) $arrayEnd )"
    val ds = tds.selectExpr(s"$group_function( $arrayStart $sub, $sub, $sub $arrayEnd ) as res")
    //ds.show()
    import com.sparkutils.quality.implicits._
    val r = ds.selectExpr("res.*").as[RuleSuiteGroupResults].collect()

    verifyRuleSuites(r)(_)
  }

  def collectorResultGroupsShouldGroup[T: TypedEncoder](conv: T => RuleSuiteGroupResults, arrayStart: String = "array(", arrayEnd: String = ")"): Seq[T] = {
    val name = register_rule_suite_group(group)
    val s = sparkSession
    val tds = {
      import s.implicits._
      testData.toDS()
    }
    val sub = s"$group_function( $arrayStart collect_runner(rule_suite_from($name, 1)), " +
      s"collect_runner(rule_suite_from($name, 2)), collect_runner(rule_suite_from($name, 3)) $arrayEnd )"
    val ds = tds.selectExpr(s"$group_function( $arrayStart $sub, $sub, $sub $arrayEnd ) as res")
    //ds.show()

    import frameless._
    implicit val enc = TypedExpressionEncoder[T]
    //ds.printSchema()

    val r = ds.selectExpr("res.*").as[T].collect()
    verifyRuleSuites(r)(conv)
    r
  }

}

class GroupResultsTest extends GroupTestBase {

  val group_function: String = "group_results"

  test("dq results should group") {
    dqResultsShouldGroup()
  }

  test("dq results shouldn't allow processing") {
    val name = register_rule_suite_group(group)
    val s = sparkSession
    import s.implicits._
    val tds = testData.toDS()

    val caught =
      intercept[Exception] {
        val ds = tds.selectExpr(s"group_results( array( dq_rule_runner(rule_suite_from($name, 1)), " +
          s"dq_rule_runner(rule_suite_from($name, 2)), dq_rule_runner(rule_suite_from($name, 3)) ), f -> f ) as res")
        ds.collect()
      }

    caught.getMessage should include("accepts the process lambda when using an array of engine")
  }

  test("engine results should group") {
    type T = (RuleSuiteGroupResults, Seq[(Option[SalientRule], Option[Seq[NewPosting]])])

    import frameless._
    implicit val tenc = TypedEncoder[T]
    engineResultShouldGroup[T](_._1)
  }

  test("folder results should group") {
    // the results of folder are optional / nullable
    type T = (RuleSuiteGroupResults, Seq[Option[Tuple1[Seq[NewPosting]]]])
    //type T = (RuleSuiteGroupResults, Seq[Option[Seq[NewPosting]]])

    import frameless._
    implicit val tenc = TypedEncoder[T]
    folderResultShouldGroup[T](_._1)
  }

  test("collector results should group") {
    // the results of folder are optional / nullable
    type T = (RuleSuiteGroupResults, Seq[Option[Seq[NewPosting]]])

    import frameless._
    implicit val tenc = TypedEncoder[T]

    collectorResultShouldGroup[T](_._1)
  }

  test("collector results should group - with flatten") {
    val name = register_rule_suite_group(group)
    val s = sparkSession
    val tds = {
      import s.implicits._
      testData.toDS()
    }

    val ds = tds.selectExpr(s"group_results( array( collect_runner(rule_suite_from($name, 1)), " +
      s"collect_runner(rule_suite_from($name, 2)), collect_runner(rule_suite_from($name, 3)) ), f -> flatten(f) ) as res")
    //ds.show()

    // the results of folder are optional / nullable
    type T = (RuleSuiteGroupResults, Option[Seq[NewPosting]])

    import frameless._
    implicit val tenc = TypedEncoder[T]
    implicit val enc = TypedExpressionEncoder[T]
    //ds.printSchema()

    val r = ds.selectExpr("res.*").as[T].collect()
    verifyRuleSuites(r)(_._1)
    // verify flatten actually worked
    r.map(_._2.get.nonEmpty) shouldBe Seq(true, false, false, true, true, true)
  }

  test("result groups should group") {
    resultGroupsShouldGroup()
  }

  test("collector results groups should group") {
    // the results of folder are optional / nullable
    type T = (RuleSuiteGroupResults, Seq[Seq[Option[Seq[NewPosting]]]])

    import frameless._
    implicit val tenc = TypedEncoder[T]

    val r = collectorResultGroupsShouldGroup[T](_._1)
    // check the collect of collect of collect had the expected results
    r.map(_._2.flatten.forall(_.get.nonEmpty)) shouldBe Seq(true, false, false, true, true, true)
  }

  test("collector results groups should group and flatten a lot") {
    val name = register_rule_suite_group(group)
    val s = sparkSession
    val tds = {
      import s.implicits._
      testData.toDS()
    }
    val sub = s"group_results( array( collect_runner(rule_suite_from($name, 1)), " +
      s"collect_runner(rule_suite_from($name, 2)), collect_runner(rule_suite_from($name, 3)) ) )"
    val ds = tds.selectExpr(s"group_results( array( $sub, $sub, $sub ), f -> flatten(flatten(f)) ) as res")
    //ds.show()

    // the results of folder are optional / nullable
    type T = (RuleSuiteGroupResults, Option[Seq[NewPosting]])

    import frameless._
    implicit val tenc = TypedEncoder[T]
    implicit val enc = TypedExpressionEncoder[T]
    //ds.printSchema()

    val r = ds.selectExpr("res.*").as[T].collect()

    verifyRuleSuites(r)(_._1)

    // verify flatten flatten actually worked
    r.map(_._2.get.nonEmpty) shouldBe Seq(true, false, false, true, true, true)
  }

  test("bad types shouldn't pass analysis") {
    val name = register_rule_suite_group(group)
    val s = sparkSession
    val tds = {
      import s.implicits._
      testData.toDS()
    }

    val badParams = Seq("1", "'b'", "array(1)", "array('b')", s"rule_suite_from($name, 1)")

    badParams.foreach {
      bad =>

        var caught =
          intercept[Exception] {

            val ds = tds.selectExpr(s"group_results( $bad )")

            ds.collect()
          }

        caught.getMessage should include("arrays of structures")

        caught =
          intercept[Exception] {

            val ds = tds.selectExpr(s"group_results( $bad, f -> flatten(f) )")

            ds.collect()
          }

        caught.getMessage should include("arrays of structures")
    }

  }

  test("unify_result should group, group and then flatten") {
    val name = register_rule_suite_group(group.copy(
      ruleSuites = group.ruleSuites.mapValues{
        rs =>
          if (rs.id.id == 3)    // folder needs a struct / row, so wrap the array up in outputs, starter and in the type
            RuleSuite.mapRules(rs){
              r =>
                val nrop = r.runOnPassProcessor.withExpr( OutputExpression( "cur -> struct(" +
                  r.runOnPassProcessor.outputExpression.asInstanceOf[HasRuleText].rule +" )"))
                r.copy( runOnPassProcessor = nrop )
            }
          else
            rs
      }.toMap
    ))
    val s = sparkSession
    val tds = {
      import s.implicits._
      testData.toDS()
    }
    val starter = ", named_struct('arr', array(struct('' as transfer_type, account, product, subcode)))"

    val sub = s"group_results( array( unify_result( collect_runner(rule_suite_from($name, 1)) ), " +
      s"unify_result( rule_engine_runner(rule_suite_from($name, 2)) ), " +
      s"unify_result( struct(rule_folder_runner(rule_suite_from($name, 3)$starter).ruleSuiteResults, " +
      s"rule_folder_runner(rule_suite_from($name, 3)$starter).result.arr as result ) ) ) )" // unpack for folder
    val ds = tds.selectExpr(s"group_results( array( $sub, unify_result( $sub ), $sub ), f -> flatten(flatten(f)) ) as res")
    //ds.show()

    // the results of folder are optional / nullable
    type T = (RuleSuiteGroupResults, Option[Seq[NewPosting]])

    import frameless._
    implicit val tenc = TypedEncoder[T]
    implicit val enc = TypedExpressionEncoder[T]
    //ds.printSchema()

    val r = ds.selectExpr("res.*").as[T].collect()

    verifyRuleSuites(r)(_._1)

    // verify flatten flatten actually worked
    r.map(_._2.nonEmpty) shouldBe Seq(true, false, false, true, true, true)
  }
}
