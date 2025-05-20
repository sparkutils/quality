package com.sparkutils.qualityTests

import com.sparkutils.quality._
import com.sparkutils.quality.impl.FlattenStruct.ruleSuiteDeserializer
import com.sparkutils.quality.sparkless.{ProcessFunctions, Processor}
import org.apache.spark.sql.{Encoders, QualitySparkUtils}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{MutableProjection, Projection}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.junit.runner.RunWith
import org.scalatest.{FunSuite, Matchers}
import org.scalatestplus.junit.JUnitRunner

import scala.beans.BeanProperty

class NewPostingBean(){
  @BeanProperty
  var transfer_type: String = _
  @BeanProperty
  var account: String = _
  @BeanProperty
  var product: String = _
  @BeanProperty
  var subcode: Int = _

  def toNewPosting() =
    NewPosting(transfer_type, account, product, subcode)
}

// purposefully NOT in the testShade as this is inappropriate for actual spark usage
@RunWith(classOf[JUnitRunner])
class RowToRowTest extends FunSuite with Matchers  with TestUtils {

  val testData=Seq(
    TestOn("edt", "4251", 50),
    TestOn("otc", "4201", 40),
    TestOn("fi", "4251", 50),
    TestOn("fx", "4206", 60),
    TestOn("fxotc", "4201", 40),
    TestOn("eqotc", "4200", 60)
  )

  val typ = StructType(Seq(
    StructField("product", StringType, nullable = false),
    StructField("account", StringType, nullable = false),
    StructField("subcode", IntegerType, nullable = false)
  ))

  test("simple projection") { not2_4 { not_Cluster { evalCodeGensNoResolve {

    def map(seq: Seq[TestOn], projection: Projection, resi: Int): Seq[Int] = seq.map{ s =>
      val i = InternalRow(UTF8String.fromString(s.product), UTF8String.fromString(s.account), s.subcode)
      val r = projection(i)
      r.getInt(resi)
    }

    val s = sparkSession // force it
    registerQualityFunctions()

    val rs = RuleSuite(Id(1,1), Seq(
      RuleSet(Id(50, 1), Seq(
        Rule(Id(100, 1), ExpressionRule("if(product like '%otc%', account = '4201', subcode = 50)"))
      ))
    ))

    val exprs = QualitySparkUtils.resolveExpressions(typ, taddOverallResultsAndDetailsF(rs))
    val iprocessor = QualitySparkUtils.rowProcessor(exprs, inCodegen).asInstanceOf[MutableProjection]
    iprocessor.target(InternalRow(null, null, null, null, null))
    iprocessor.initialize(0)
    val cprocessor = QualitySparkUtils.rowProcessor(exprs, inCodegen).asInstanceOf[MutableProjection]
    cprocessor.target(InternalRow(null, null, null, null, null))
    cprocessor.initialize(0)

    val ro = map(testData, iprocessor, exprs.length - 2)
    ro shouldBe Seq(PassedInt, PassedInt, PassedInt, FailedInt, PassedInt, FailedInt)
    val rc = map(testData, cprocessor, exprs.length - 2)
    rc shouldBe Seq(PassedInt, PassedInt, PassedInt, FailedInt, PassedInt, FailedInt)
  } } } }

  test("encoder output projection") { not2_4 { not_Cluster { evalCodeGensNoResolve {
    val s = sparkSession // force it
    registerQualityFunctions()

    val enc = QualitySparkUtils.rowProcessor(Seq(ruleSuiteDeserializer), inCodegen).asInstanceOf[MutableProjection]
    enc.target(InternalRow(null))

    def map(seq: Seq[TestOn], projection: Projection, resi: Int): Seq[RuleSuiteResult] = seq.map{ s =>
      val i = InternalRow(UTF8String.fromString(s.product), UTF8String.fromString(s.account), s.subcode)
      val r = projection(i)
      enc(r.getStruct(resi, 2)).get(0, ObjectType(classOf[RuleSuiteResult])).asInstanceOf[RuleSuiteResult]
    }

    val rs = RuleSuite(Id(1,1), Seq(
      RuleSet(Id(50, 1), Seq(
        Rule(Id(100, 1), ExpressionRule("if(product like '%otc%', account = '4201', subcode = 50)"))
      ))
    ))

    val exprs = QualitySparkUtils.resolveExpressions(typ, taddDataQualityF(rs))
    val iprocessor = QualitySparkUtils.rowProcessor(exprs, false).asInstanceOf[MutableProjection]
    iprocessor.target(InternalRow(null, null, null, null, null))
    iprocessor.initialize(0)
    val cprocessor = QualitySparkUtils.rowProcessor(exprs, true).asInstanceOf[MutableProjection]
    cprocessor.target(InternalRow(null, null, null, null, null))
    cprocessor.initialize(0)

    val ro = map(testData, iprocessor, exprs.length - 1)
    ro.map(_.overallResult) shouldBe Seq(Passed, Passed, Passed, Failed, Passed, Failed)
    val rc = map(testData, cprocessor, exprs.length - 1)
    rc.map(_.overallResult)  shouldBe Seq(Passed, Passed, Passed, Failed, Passed, Failed)
  } } } }

  test("via ProcessFactory") { not2_4 { not_Cluster { evalCodeGensNoResolve {
    val s = sparkSession // force it

    import s.implicits._

    def map(seq: Seq[TestOn], process: Processor[TestOn, RuleSuiteResult]): Seq[RuleSuiteResult] = seq.map{ s =>
      process(s)
    }

    val rs = RuleSuite(Id(1,1), Seq(
      RuleSet(Id(50, 1), Seq(
        Rule(Id(100, 1), ExpressionRule("if(product like '%otc%', account = '4201', subcode = 50)"))
      ))
    ))

    val processor = ProcessFunctions.dqFactory[TestOn](rs, inCodegen).instance

    val ro = map(testData, processor)
    ro.map(_.overallResult) shouldBe Seq(Passed, Passed, Passed, Failed, Passed, Failed)
    val rc = map(testData, processor)
    rc.map(_.overallResult)  shouldBe Seq(Passed, Passed, Passed, Failed, Passed, Failed)
  } } } }

  test("via ProcessFactory rule details") { not2_4 { not_Cluster { evalCodeGensNoResolve {
    val s = sparkSession // force it

    import s.implicits._

    def map(seq: Seq[TestOn], process: Processor[TestOn, (RuleResult, RuleSuiteResultDetails)]): Seq[RuleResult] = seq.map{ s =>
      process(s)._1
    }

    val rs = RuleSuite(Id(1,1), Seq(
      RuleSet(Id(50, 1), Seq(
        Rule(Id(100, 1), ExpressionRule("if(product like '%otc%', account = '4201', subcode = 50)"))
      ))
    ))

    val processor = ProcessFunctions.dqDetailsFactory[TestOn](rs, inCodegen).instance

    val ro = map(testData, processor)
    ro shouldBe Seq(Passed, Passed, Passed, Failed, Passed, Failed)
    val rc = map(testData, processor)
    rc shouldBe Seq(Passed, Passed, Passed, Failed, Passed, Failed)
  } } } }

  test("via ProcessFactory rule engine") { not2_4_or_3_0_or_3_1 { not_Cluster { evalCodeGensNoResolve {
    val s = sparkSession // force it

    import s.implicits._

    val DDL = "ARRAY<STRUCT<`transfer_type`: STRING, `account`: STRING, `product`: STRING, `subcode`: INTEGER >>"
    registerLambdaFunctions(Seq(
      LambdaFunction("account_row", "(transfer_type, account) -> named_struct('transfer_type', transfer_type, 'account', account, 'product', product, 'subcode', subcode)", Id(123, 23)),
      LambdaFunction("account_row", "transfer_type -> account_row(transfer_type, account)", Id(123, 24)),
      LambdaFunction("subcode", "(transfer_type, sub) -> updateField(account_row(transfer_type, account), 'subcode', sub)", Id(123, 25))
    ))

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
    val ruleSuite = RuleSuite(rsId, Seq(
      RuleSet(Id(50, 1), rules
      )))

    def map(seq: Seq[TestOn], process: Processor[TestOn, RuleEngineResult[Seq[NewPosting]]]): Seq[RuleEngineResult[Seq[NewPosting]]] = seq.map{ s =>
      process(s)
    }

    import com.sparkutils.quality.implicits._

    val processor = ProcessFunctions.ruleEngineFactory[TestOn, Seq[NewPosting]](ruleSuite, DataType.fromDDL(DDL), compile = inCodegen).instance

    val res = map(testData, processor)

    // first three all failed
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
  } } } }

  test("via ProcessFactory rule engine T array") { not2_4_or_3_0_or_3_1 { not_Cluster { evalCodeGensNoResolve {
    val s = sparkSession // force it

    import s.implicits._

    val DDL = "ARRAY<STRUCT<`transfer_type`: STRING, `account`: STRING, `product`: STRING, `subcode`: INTEGER >>"
    registerLambdaFunctions(Seq(
      LambdaFunction("account_row", "(transfer_type, account) -> named_struct('transfer_type', transfer_type, 'account', account, 'product', product, 'subcode', subcode)", Id(123, 23)),
      LambdaFunction("account_row", "transfer_type -> account_row(transfer_type, account)", Id(123, 24)),
      LambdaFunction("subcode", "(transfer_type, sub) -> updateField(account_row(transfer_type, account), 'subcode', sub)", Id(123, 25))
    ))

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
    val ruleSuite = RuleSuite(rsId, Seq(
      RuleSet(Id(50, 1), rules
      )))

    def map(seq: Seq[TestOn], process: Processor[TestOn, RuleEngineResult[Seq[NewPosting]]]): Seq[RuleEngineResult[Seq[NewPosting]]] = seq.map{ s =>
      process(s)
    }

    val processor = ProcessFunctions.ruleEngineFactoryT[TestOn, Seq[NewPosting]](ruleSuite, DataType.fromDDL(DDL), compile = inCodegen).instance

    val res = map(testData, processor)

    // first three all failed
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
  } } } }

  test("via ProcessFactory rule engine T product") { not2_4_or_3_0_or_3_1 { not_Cluster { evalCodeGensNoResolve {
    val s = sparkSession // force it

    import s.implicits._

    val DDL = "STRUCT<`transfer_type`: STRING, `account`: STRING, `product`: STRING, `subcode`: INTEGER >"
    registerLambdaFunctions(Seq(
      LambdaFunction("account_row", "(transfer_type, account) -> named_struct('transfer_type', transfer_type, 'account', account, 'product', product, 'subcode', subcode)", Id(123, 23)),
      LambdaFunction("account_row", "transfer_type -> account_row(transfer_type, account)", Id(123, 24)),
      LambdaFunction("subcode", "(transfer_type, sub) -> updateField(account_row(transfer_type, account), 'subcode', sub)", Id(123, 25))
    ))

    val expressionRules = Seq((ExpressionRule("product = 'edt' and subcode = 40"), RunOnPassProcessor(1000, Id(1040,1),
      OutputExpression("account_row('from'), account_row('to', 'other_account1')"))),
      (ExpressionRule("product like '%fx%'"), RunOnPassProcessor(1000, Id(1042,1),
        OutputExpression("named_struct('transfer_type', 'from', 'account', 'another_account', 'product', product, 'subcode', subcode)"))),
      (ExpressionRule("product = 'eqotc'"), RunOnPassProcessor(1000, Id(1043,1),
        OutputExpression("subcode('fromWithField', 6000)")))
    )

    val rules =
      for { ((exp, processor), idOffset) <- expressionRules.zipWithIndex }
        yield Rule(Id(100 * idOffset, 1), exp, processor)

    val rsId = Id(1, 1)
    val ruleSuite = RuleSuite(rsId, Seq(
      RuleSet(Id(50, 1), rules
      )))

    def map(seq: Seq[TestOn], process: Processor[TestOn, RuleEngineResult[NewPosting]]): Seq[RuleEngineResult[NewPosting]] = seq.map{ s =>
      process(s)
    }

    val processor = ProcessFunctions.ruleEngineFactoryT[TestOn, NewPosting](ruleSuite, DataType.fromDDL(DDL), compile = inCodegen).instance

    val res = map(testData, processor)

    // first three all failed
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

    res(3).result shouldBe Some(NewPosting("from", "another_account", "fx", 60))
    res(3).salientRule shouldBe Some(SalientRule(Id(1,1),Id(50,1),Id(100,1)))

    res(4).result shouldBe Some(NewPosting("from", "another_account", "fxotc", 40))
    res(4).salientRule shouldBe Some(SalientRule(Id(1,1),Id(50,1),Id(100,1)))

    res(5).result shouldBe Some(NewPosting("fromWithField", "4200", "eqotc", 6000))
    res(5).salientRule shouldBe Some(SalientRule(Id(1,1),Id(50,1),Id(200,1)))
  } } } }


  test("via ProcessFactory rule engine product") { not2_4_or_3_0_or_3_1 { not_Cluster { evalCodeGensNoResolve {
    val s = sparkSession // force it

    import s.implicits._

    val DDL = "STRUCT<`transfer_type`: STRING, `account`: STRING, `product`: STRING, `subcode`: INTEGER >"
    registerLambdaFunctions(Seq(
      LambdaFunction("account_row", "(transfer_type, account) -> named_struct('transfer_type', transfer_type, 'account', account, 'product', product, 'subcode', subcode)", Id(123, 23)),
      LambdaFunction("account_row", "transfer_type -> account_row(transfer_type, account)", Id(123, 24)),
      LambdaFunction("subcode", "(transfer_type, sub) -> updateField(account_row(transfer_type, account), 'subcode', sub)", Id(123, 25))
    ))

    val expressionRules = Seq((ExpressionRule("product = 'edt' and subcode = 40"), RunOnPassProcessor(1000, Id(1040,1),
      OutputExpression("account_row('from'), account_row('to', 'other_account1')"))),
      (ExpressionRule("product like '%fx%'"), RunOnPassProcessor(1000, Id(1042,1),
        OutputExpression("named_struct('transfer_type', 'from', 'account', 'another_account', 'product', product, 'subcode', subcode)"))),
      (ExpressionRule("product = 'eqotc'"), RunOnPassProcessor(1000, Id(1043,1),
        OutputExpression("subcode('fromWithField', 6000)")))
    )

    val rules =
      for { ((exp, processor), idOffset) <- expressionRules.zipWithIndex }
        yield Rule(Id(100 * idOffset, 1), exp, processor)

    val rsId = Id(1, 1)
    val ruleSuite = RuleSuite(rsId, Seq(
      RuleSet(Id(50, 1), rules
      )))

    def map(seq: Seq[TestOn], process: Processor[TestOn, RuleEngineResult[NewPosting]]): Seq[RuleEngineResult[NewPosting]] = seq.map{ s =>
      process(s)
    }

    import com.sparkutils.quality.implicits._

    val processor = ProcessFunctions.ruleEngineFactory[TestOn, NewPosting](ruleSuite, DataType.fromDDL(DDL), compile = inCodegen).instance

    val res = map(testData, processor)

    // first three all failed
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

    res(3).result shouldBe Some(NewPosting("from", "another_account", "fx", 60))
    res(3).salientRule shouldBe Some(SalientRule(Id(1,1),Id(50,1),Id(100,1)))

    res(4).result shouldBe Some(NewPosting("from", "another_account", "fxotc", 40))
    res(4).salientRule shouldBe Some(SalientRule(Id(1,1),Id(50,1),Id(100,1)))

    res(5).result shouldBe Some(NewPosting("fromWithField", "4200", "eqotc", 6000))
    res(5).salientRule shouldBe Some(SalientRule(Id(1,1),Id(50,1),Id(200,1)))
  } } } }

  test("via ProcessFactory rule engine T bean") { not2_4_or_3_0_or_3_1 { not_Cluster { evalCodeGensNoResolve {
    val s = sparkSession // force it

    import s.implicits._

    val DDL = "STRUCT<`transfer_type`: STRING, `account`: STRING, `product`: STRING, `subcode`: INTEGER >"
    registerLambdaFunctions(Seq(
      LambdaFunction("account_row", "(transfer_type, account) -> named_struct('transfer_type', transfer_type, 'account', account, 'product', product, 'subcode', subcode)", Id(123, 23)),
      LambdaFunction("account_row", "transfer_type -> account_row(transfer_type, account)", Id(123, 24)),
      LambdaFunction("subcode", "(transfer_type, sub) -> updateField(account_row(transfer_type, account), 'subcode', sub)", Id(123, 25))
    ))

    val expressionRules = Seq((ExpressionRule("product = 'edt' and subcode = 40"), RunOnPassProcessor(1000, Id(1040,1),
      OutputExpression("account_row('from'), account_row('to', 'other_account1')"))),
      (ExpressionRule("product like '%fx%'"), RunOnPassProcessor(1000, Id(1042,1),
        OutputExpression("named_struct('transfer_type', 'from', 'account', 'another_account', 'product', product, 'subcode', subcode)"))),
      (ExpressionRule("product = 'eqotc'"), RunOnPassProcessor(1000, Id(1043,1),
        OutputExpression("subcode('fromWithField', 6000)")))
    )

    val rules =
      for { ((exp, processor), idOffset) <- expressionRules.zipWithIndex }
        yield Rule(Id(100 * idOffset, 1), exp, processor)

    val rsId = Id(1, 1)
    val ruleSuite = RuleSuite(rsId, Seq(
      RuleSet(Id(50, 1), rules
      )))

    def map(seq: Seq[TestOn], process: Processor[TestOn, RuleEngineResult[NewPostingBean]]): Seq[RuleEngineResult[NewPostingBean]] = seq.map{ s =>
      process(s)
    }

    implicit val beany = Encoders.bean(classOf[NewPostingBean])

    val processor = ProcessFunctions.ruleEngineFactoryT[TestOn, NewPostingBean](ruleSuite, DataType.fromDDL(DDL), compile = inCodegen).instance

    val res = map(testData, processor)

    // first three all failed
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

    res(3).result.map(_.toNewPosting()) shouldBe Some(NewPosting("from", "another_account", "fx", 60))
    res(3).salientRule shouldBe Some(SalientRule(Id(1,1),Id(50,1),Id(100,1)))

    res(4).result.map(_.toNewPosting()) shouldBe Some(NewPosting("from", "another_account", "fxotc", 40))
    res(4).salientRule shouldBe Some(SalientRule(Id(1,1),Id(50,1),Id(100,1)))

    res(5).result.map(_.toNewPosting()) shouldBe Some(NewPosting("fromWithField", "4200", "eqotc", 6000))
    res(5).salientRule shouldBe Some(SalientRule(Id(1,1),Id(50,1),Id(200,1)))
  } } } }


  test("via ProcessFactory rule engine T string") { not2_4_or_3_0_or_3_1 { not_Cluster { evalCodeGensNoResolve {
    val s = sparkSession // force it

    import s.implicits._

    val DDL = "STRING"

    val expressionRules = Seq((ExpressionRule("product = 'edt' and subcode = 40"), RunOnPassProcessor(1000, Id(1040,1),
      OutputExpression("'other_account1'"))),
      (ExpressionRule("product like '%fx%'"), RunOnPassProcessor(1000, Id(1042,1),
        OutputExpression("'from'"))),
      (ExpressionRule("product = 'eqotc'"), RunOnPassProcessor(1000, Id(1043,1),
        OutputExpression("'fromWithField'")))
    )

    val rules =
      for { ((exp, processor), idOffset) <- expressionRules.zipWithIndex }
        yield Rule(Id(100 * idOffset, 1), exp, processor)

    val rsId = Id(1, 1)
    val ruleSuite = RuleSuite(rsId, Seq(
      RuleSet(Id(50, 1), rules
      )))

    def map(seq: Seq[TestOn], process: Processor[TestOn, RuleEngineResult[String]]): Seq[RuleEngineResult[String]] = seq.map{ s =>
      process(s)
    }

    val processor = ProcessFunctions.ruleEngineFactoryT[TestOn, String](ruleSuite, DataType.fromDDL(DDL), compile = inCodegen).instance

    val res = map(testData, processor)

    // first three all failed
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

    res(3).result shouldBe Some("from")
    res(3).salientRule shouldBe Some(SalientRule(Id(1,1),Id(50,1),Id(100,1)))

    res(4).result shouldBe Some("from")
    res(4).salientRule shouldBe Some(SalientRule(Id(1,1),Id(50,1),Id(100,1)))

    res(5).result shouldBe Some("fromWithField")
    res(5).salientRule shouldBe Some(SalientRule(Id(1,1),Id(50,1),Id(200,1)))
  } } } }

  test("via ProcessFactory rule engine T map") { not2_4_or_3_0_or_3_1 { not_Cluster { evalCodeGensNoResolve {
    val s = sparkSession // force it

    import s.implicits._

    val DDL = "map<STRING, String>"

    val expressionRules = Seq((ExpressionRule("product = 'edt' and subcode = 40"), RunOnPassProcessor(1000, Id(1040,1),
      OutputExpression("map('transfer', 'other_account1')"))),
      (ExpressionRule("product like '%fx%'"), RunOnPassProcessor(1000, Id(1042,1),
        OutputExpression("map('transfer', 'from')"))),
      (ExpressionRule("product = 'eqotc'"), RunOnPassProcessor(1000, Id(1043,1),
        OutputExpression("map('transfer', 'fromWithField')")))
    )

    val rules =
      for { ((exp, processor), idOffset) <- expressionRules.zipWithIndex }
        yield Rule(Id(100 * idOffset, 1), exp, processor)

    val rsId = Id(1, 1)
    val ruleSuite = RuleSuite(rsId, Seq(
      RuleSet(Id(50, 1), rules
      )))

    def map(seq: Seq[TestOn], process: Processor[TestOn, RuleEngineResult[Map[String,String]]]): Seq[RuleEngineResult[Map[String,String]]] = seq.map{ s =>
      process(s)
    }

    val processor = ProcessFunctions.ruleEngineFactoryT[TestOn, Map[String,String]](ruleSuite, DataType.fromDDL(DDL), compile = inCodegen).instance

    val res = map(testData, processor)

    // first three all failed
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

    res(3).result shouldBe Some(Map("transfer" -> "from"))
    res(3).salientRule shouldBe Some(SalientRule(Id(1,1),Id(50,1),Id(100,1)))

    res(4).result shouldBe Some(Map("transfer" -> "from"))
    res(4).salientRule shouldBe Some(SalientRule(Id(1,1),Id(50,1),Id(100,1)))

    res(5).result shouldBe Some(Map("transfer" -> "fromWithField"))
    res(5).salientRule shouldBe Some(SalientRule(Id(1,1),Id(50,1),Id(200,1)))
  } } } }

  test("via ProcessFactory folder engine T product") { not2_4_or_3_0_or_3_1 { not_Cluster { evalCodeGensNoResolve {
    val s = sparkSession // force it

    import s.implicits._

    val DDL = "STRUCT<`account`: STRING, `product`: STRING, `subcode`: INTEGER >"
    registerLambdaFunctions(Seq(
      LambdaFunction("account_row", "(transfer_type, account) -> named_struct('account', account, 'product', transfer_type, 'subcode', subcode)", Id(123, 23)),
      LambdaFunction("account_row", "transfer_type -> account_row(transfer_type, account)", Id(123, 24)),
      LambdaFunction("subcode", "(transfer_type, sub) -> updateField(account_row(transfer_type, account), 'subcode', sub)", Id(123, 25))
    ))

    val expressionRules = Seq((ExpressionRule("product = 'edt' and subcode = 40"), RunOnPassProcessor(1000, Id(1040, 1),
      OutputExpression("thecurrent -> updateField(updateField(thecurrent, 'subcode', 1234), 'account', 'from')"))),

      (ExpressionRule("product like '%fx%'"), RunOnPassProcessor(1000, Id(1042, 1),
        OutputExpression("thecurrent -> updateField(thecurrent, 'product', 'to')"))),
      (ExpressionRule("product = 'eqotc'"), RunOnPassProcessor(1000, Id(1043, 1),
        OutputExpression("thecurrent -> updateField(thecurrent, 'product', 'from')"))),
      (ExpressionRule("product = 'eqotc'"), RunOnPassProcessor(1001, Id(1044, 1),
        OutputExpression("thecurrent -> update_field(thecurrent, 'account', concat(account,'_fruit'))")))
    )

    val rules =
      for { ((exp, processor), idOffset) <- expressionRules.zipWithIndex }
        yield Rule(Id(100 * idOffset, 1), exp, processor)

    val rsId = Id(1, 1)
    val ruleSuite = RuleSuite(rsId, Seq(
      RuleSet(Id(50, 1), rules
      )))

    def map(seq: Seq[TestOn], process: Processor[TestOn, RuleFolderResult[TestOn]]): Seq[RuleFolderResult[TestOn]] = seq.map{ s =>
      process(s)
    }

    val processor = ProcessFunctions.ruleFolderFactoryT[TestOn, TestOn](ruleSuite, DataType.fromDDL(DDL).asInstanceOf[StructType],
      compile = inCodegen).instance

    val res = map(testData, processor)

    // first three all failed
    for(i <- 0 until 3) {
      res(i).result.isEmpty shouldBe true
      res(i).ruleSuiteResults.overallResult shouldBe Failed
    }
    for(i <- 3 until 6) {
      res(i).result.isDefined shouldBe true
      res(i).ruleSuiteResults.overallResult shouldBe Failed
    }

    res(3).result shouldBe Some(TestOn("to", "4206", 60))

    res(4).result shouldBe Some(TestOn("to", "4201", 40))

    res(5).result shouldBe Some(TestOn("from", "4200_fruit", 60))
  } } } }


  test("via ProcessFactory folder engine T bean extra output fields") { not2_4_or_3_0_or_3_1 { not_Cluster { evalCodeGensNoResolve {
    val s = sparkSession // force it

    import s.implicits._

    val DDL = "STRUCT<`transfer_type`: STRING, `account`: STRING, `product`: STRING, `subcode`: INTEGER >"
    registerLambdaFunctions(Seq(
      LambdaFunction("account_row", "(transfer_type, account) -> named_struct('transfer_type', transfer_type, 'account', account, 'product', product, 'subcode', subcode)", Id(123, 23)),
      LambdaFunction("account_row", "transfer_type -> account_row(transfer_type, account)", Id(123, 24)),
      LambdaFunction("subcode", "(transfer_type, sub) -> updateField(account_row(transfer_type, account), 'subcode', sub)", Id(123, 25))
    ))

    val expressionRules = Seq((ExpressionRule("product = 'edt' and subcode = 40"), RunOnPassProcessor(1000, Id(1040, 1),
      OutputExpression("thecurrent -> updateField(updateField(thecurrent, 'subcode', 1234), 'account', 'from')"))),

      (ExpressionRule("product like '%fx%'"), RunOnPassProcessor(1000, Id(1042, 1),
        OutputExpression("thecurrent -> updateField(thecurrent, 'transfer_type', 'to')"))),
      (ExpressionRule("product = 'eqotc'"), RunOnPassProcessor(1000, Id(1043, 1),
        OutputExpression("thecurrent -> updateField(thecurrent, 'transfer_type', 'from')"))),
      (ExpressionRule("product = 'eqotc'"), RunOnPassProcessor(1001, Id(1044, 1),
        OutputExpression("thecurrent -> update_field(thecurrent, 'account', concat(account,'_fruit'))")))
    )

    val rules =
      for { ((exp, processor), idOffset) <- expressionRules.zipWithIndex }
        yield Rule(Id(100 * idOffset, 1), exp, processor)

    val rsId = Id(1, 1)
    val ruleSuite = RuleSuite(rsId, Seq(
      RuleSet(Id(50, 1), rules
      )))

    def map(seq: Seq[TestOn], process: Processor[TestOn, RuleFolderResult[NewPostingBean]]): Seq[RuleFolderResult[NewPostingBean]] = seq.map{ s =>
      process(s)
    }

    implicit val beany = Encoders.bean(classOf[NewPostingBean])

    val processor = ProcessFunctions.ruleFolderFactoryWithStructStarterT[TestOn, NewPostingBean](ruleSuite,
      Seq(("transfer_type", lit("dummy")), ("account", col("account")), ("product", col("product")), ("subcode", col("subcode"))),
      DataType.fromDDL(DDL).asInstanceOf[StructType],
      compile = inCodegen).instance

    val res = map(testData, processor)

    // first three all failed
    for(i <- 0 until 3) {
      res(i).result.isEmpty shouldBe true
      res(i).ruleSuiteResults.overallResult shouldBe Failed
    }
    for(i <- 3 until 6) {
      res(i).result.isDefined shouldBe true
      res(i).ruleSuiteResults.overallResult shouldBe Failed
    }

    res(3).result.map(_.toNewPosting()) shouldBe Some(NewPosting("to", "4206", "fx", 60))

    res(4).result.map(_.toNewPosting()) shouldBe Some(NewPosting("to", "4201", "fxotc", 40))

    res(5).result.map(_.toNewPosting()) shouldBe Some(NewPosting("from", "4200_fruit", "eqotc", 60))
  } } } }


}
