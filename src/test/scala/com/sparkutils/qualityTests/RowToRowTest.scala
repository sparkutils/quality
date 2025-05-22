package com.sparkutils.qualityTests

import com.sparkutils.quality._
import com.sparkutils.quality.impl.FlattenStruct.ruleSuiteDeserializer
import com.sparkutils.quality.sparkless.impl.LocalBroadcast
import com.sparkutils.quality.sparkless.impl.Processors.NO_QUERY_PLANS
import com.sparkutils.quality.sparkless.{ProcessFunctions, Processor}
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.spark.sql.{Encoders, QualitySparkUtils}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{MutableProjection, Projection}
import org.apache.spark.sql.functions.{col, column, lit}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import org.scalatestplus.junit.JUnitRunner

import java.io.ByteArrayOutputStream
import scala.beans.BeanProperty
import scala.collection.JavaConverters._

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
class RowToRowTest extends FunSuite with Matchers with BeforeAndAfterAll with TestUtils {

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

  override protected def beforeAll(): Unit = {
    val s = sparkSession // force it
    registerQualityFunctions()
  }

  test("simple projection") { not2_4 { not_Cluster { evalCodeGensNoResolve {
    def map(seq: Seq[TestOn], projection: Projection, resi: Int): Seq[Int] = seq.map{ s =>
      val i = InternalRow(UTF8String.fromString(s.product), UTF8String.fromString(s.account), s.subcode)
      val r = projection(i)
      r.getInt(resi)
    }

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

  def map[I,O](seq: Seq[I], process: Processor[I, O]): Seq[O] = seq.map{ s =>
    process(s)
  }

  test("via ProcessFactory") { not2_4 { not_Cluster { evalCodeGensNoResolve {
    import sparkSession.implicits._

    val rs = RuleSuite(Id(1,1), Seq(
      RuleSet(Id(50, 1), Seq(
        Rule(Id(100, 1), ExpressionRule("if(product like '%otc%', account = '4201', subcode = 50)"))
      ))
    ))

    val processor = ProcessFunctions.dqFactory[TestOn](rs, inCodegen).instance

    val rc = map(testData, processor)
    rc.map(_.overallResult)  shouldBe Seq(Passed, Passed, Passed, Failed, Passed, Failed)
    rc.map(_.getRuleSetResults.asScala.flatMap(_._2.getRuleResults.asScala)) shouldBe rc.map(_.ruleSetResults.flatMap(_._2.ruleResults))
  } } } }

  test("via ProcessFactory rule details") { not2_4 { not_Cluster { evalCodeGensNoResolve {
    import sparkSession.implicits._

    val rs = RuleSuite(Id(1,1), Seq(
      RuleSet(Id(50, 1), Seq(
        Rule(Id(100, 1), ExpressionRule("if(product like '%otc%', account = '4201', subcode = 50)"))
      ))
    ))

    val processor = ProcessFunctions.dqDetailsFactory[TestOn](rs, inCodegen).instance

    val rc = map(testData, processor)
    rc.map(_._1) shouldBe Seq(Passed, Passed, Passed, Failed, Passed, Failed)
    rc.map(_._2.getRuleSetResults.asScala.toMap) shouldBe rc.map(_._2.ruleSetResults)
  } } } }

  test("via ProcessFactory rule engine") { not2_4_or_3_0_or_3_1 { not_Cluster { evalCodeGensNoResolve {
    import sparkSession.implicits._

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
    import sparkSession.implicits._

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
    import sparkSession.implicits._

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
    import sparkSession.implicits._

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
    import sparkSession.implicits._

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
    import sparkSession.implicits._

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
    import sparkSession.implicits._

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

    val processor = ProcessFunctions.ruleEngineFactoryT[TestOn, Map[String,String]](ruleSuite, DataType.fromDDL(DDL), compile = inCodegen).instance

    val res = map(testData, processor)

    res.map(t => Option(t.getResult.orElse(null))) shouldBe res.map(_.result)
    res.map(t => Option(t.getSalientRule.orElse(null))) shouldBe res.map(_.salientRule)

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
    import sparkSession.implicits._

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

    val processor = ProcessFunctions.ruleFolderFactoryT[TestOn, TestOn](ruleSuite, DataType.fromDDL(DDL).asInstanceOf[StructType],
      compile = inCodegen).instance

    val res = map(testData, processor)

    res.map(t => Option(t.getResult.orElse(null))) shouldBe res.map(_.result)
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
    import sparkSession.implicits._

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


  test("via ProcessFactory expression T ") { not2_4_or_3_0_or_3_1 { not_Cluster { evalCodeGensNoResolve {
    import sparkSession.implicits._

    val rs = RuleSuite(Id(10, 2), Seq(RuleSet(Id(20, 1), Seq(
      Rule(Id(30, 3), ExpressionRule("account like '42%'")),
      Rule(Id(31, 3), ExpressionRule("product like 'fx%'")),
      Rule(Id(32, 3), ExpressionRule("subcode = 40"))
    ))))

    implicit val bool = Encoders.BOOLEAN

    val processor = ProcessFunctions.expressionRunnerFactoryT[TestOn, Boolean](rs, BooleanType,
      compile = inCodegen).instance

    val res = map(testData, processor)
    res.map(_.getRuleSetResults.asScala) shouldBe res.map(_.ruleSetResults)
    res.map(_.ruleSetResults(Id(20,1))) shouldBe Seq(
      Map(
        Id(30, 3) -> true,
        Id(31, 3) -> false,
        Id(32, 3) -> false
      ),
      Map(
        Id(30, 3) -> true,
        Id(31, 3) -> false,
        Id(32, 3) -> true
      ),Map(
        Id(30, 3) -> true,
        Id(31, 3) -> false,
        Id(32, 3) -> false
      ),Map(
        Id(30, 3) -> true,
        Id(31, 3) -> true,
        Id(32, 3) -> false
      ),Map(
        Id(30, 3) -> true,
        Id(31, 3) -> true,
        Id(32, 3) -> true
      ),Map(
        Id(30, 3) -> true,
        Id(31, 3) -> false,
        Id(32, 3) -> false
      )
    )
  } } } }

  test("via ProcessFactory expression yaml") { not2_4_or_3_0_or_3_1 { not_Cluster { evalCodeGensNoResolve {
    import sparkSession.implicits._

    val rs = RuleSuite(Id(10, 2), Seq(RuleSet(Id(20, 1), Seq(
      Rule(Id(30, 3), ExpressionRule("account like '42%'")),
      Rule(Id(31, 3), ExpressionRule("product")),
      Rule(Id(32, 3), ExpressionRule("subcode"))
    ))))

    val processor = ProcessFunctions.expressionYamlRunnerFactory[TestOn](rs, compile = inCodegen).instance

    val res = map(testData, processor)

    val STRING = "STRING"
    val BOOLEAN = "BOOLEAN"
    val INT = "INT"

    res.map(_.ruleSetResults(Id(20,1))) shouldBe Seq(
      Map(
        Id(30, 3) -> GeneralExpressionResult("true\n", BOOLEAN),
        Id(31, 3) -> GeneralExpressionResult("edt\n", STRING),
        Id(32, 3) -> GeneralExpressionResult("50\n", INT)
      ),
      Map(
        Id(30, 3) -> GeneralExpressionResult("true\n", BOOLEAN),
        Id(31, 3) -> GeneralExpressionResult("otc\n", STRING),
        Id(32, 3) -> GeneralExpressionResult("40\n", INT)
      ),Map(
        Id(30, 3) -> GeneralExpressionResult("true\n", BOOLEAN),
        Id(31, 3) -> GeneralExpressionResult("fi\n", STRING),
        Id(32, 3) -> GeneralExpressionResult("50\n", INT)
      ),Map(
        Id(30, 3) -> GeneralExpressionResult("true\n", BOOLEAN),
        Id(31, 3) -> GeneralExpressionResult("fx\n", STRING),
        Id(32, 3) -> GeneralExpressionResult("60\n", INT)
      ),Map(
        Id(30, 3) -> GeneralExpressionResult("true\n", BOOLEAN),
        Id(31, 3) -> GeneralExpressionResult("fxotc\n", STRING),
        Id(32, 3) -> GeneralExpressionResult("40\n", INT)
      ),Map(
        Id(30, 3) -> GeneralExpressionResult("true\n", BOOLEAN),
        Id(31, 3) -> GeneralExpressionResult("eqotc\n", STRING),
        Id(32, 3) -> GeneralExpressionResult("60\n", INT)
      )
    )
  } } } }

  test("via ProcessFactory expression yaml noddl") { not2_4_or_3_0_or_3_1 { not_Cluster { evalCodeGensNoResolve {
    import sparkSession.implicits._

    val rs = RuleSuite(Id(10, 2), Seq(RuleSet(Id(20, 1), Seq(
      Rule(Id(30, 3), ExpressionRule("account like '42%'")),
      Rule(Id(31, 3), ExpressionRule("product")),
      Rule(Id(32, 3), ExpressionRule("subcode"))
    ))))

    val processor = ProcessFunctions.expressionYamlNoDDLRunnerFactory[TestOn](rs, compile = inCodegen).instance

    val res = map(testData, processor)

    res.map(_.getRuleSetResults.asScala) shouldBe res.map(_.ruleSetResults)
    res.map(_.ruleSetResults(Id(20,1))) shouldBe Seq(
      Map(
        Id(30, 3) -> "true\n",
        Id(31, 3) -> "edt\n",
        Id(32, 3) -> "50\n"
      ),
      Map(
        Id(30, 3) -> "true\n",
        Id(31, 3) -> "otc\n",
        Id(32, 3) -> "40\n"
      ),Map(
        Id(30, 3) -> "true\n",
        Id(31, 3) -> "fi\n",
        Id(32, 3) -> "50\n"
      ),Map(
        Id(30, 3) -> "true\n",
        Id(31, 3) -> "fx\n",
        Id(32, 3) -> "60\n"
      ),Map(
        Id(30, 3) -> "true\n",
        Id(31, 3) -> "fxotc\n",
        Id(32, 3) -> "40\n"
      ),Map(
        Id(30, 3) -> "true\n",
        Id(31, 3) -> "eqotc\n",
        Id(32, 3) -> "60\n"
      )
    )
  } } } }


  test("prove processors can't have subqueries") { not2_4_or_3_0_or_3_1 { not_Cluster { evalCodeGensNoResolve {
    import sparkSession.implicits._

    testData.toDS.createOrReplaceTempView("testData")

    val rs = RuleSuite(Id(10, 2), Seq(RuleSet(Id(20, 1), Seq(
      Rule(Id(30, 3), ExpressionRule("select max(account) from testData t where t.account = account")),
      Rule(Id(31, 3), ExpressionRule("product")),
      Rule(Id(32, 3), ExpressionRule("subcode"))
    ))))

    val e = intercept[QualityException] {
      ProcessFunctions.expressionYamlNoDDLRunnerFactory[TestOn](rs, compile = inCodegen).instance
    }
    e.msg shouldBe NO_QUERY_PLANS
  } } } }

  test("via ProcessFactory with Avro inputs") { not2_4 { not_Cluster { evalCodeGensNoResolve {
    val testOnAvro = SchemaBuilder.record("testOnAvro")
      .namespace("com.teston")
      .fields()
      .requiredString("product")
      .requiredString("account")
      .requiredInt("subcode")
      .endRecord()
    val datumWriter = new GenericDatumWriter[GenericRecord](testOnAvro);

    val bos = new ByteArrayOutputStream()
    val enc = EncoderFactory.get().binaryEncoder(bos, null)

    val avroTestData = testData.map{d =>
      val r = new GenericData.Record(testOnAvro)
      r.put("product", d.product)
      r.put("account", d.account)
      r.put("subcode", d.subcode)
      datumWriter.write(r, enc)
      enc.flush()
      val ba = bos.toByteArray
      bos.reset()
      ba
    }

    import sparkSession.implicits._

    val rs = RuleSuite(Id(1,1), Seq(
      RuleSet(Id(50, 1), Seq(
        Rule(Id(100, 1), ExpressionRule("if(product like '%otc%', account = '4201', subcode = 50)"))
      ))
    ))

    val processor = ProcessFunctions.dqFactory[Array[Byte]](rs, inCodegen, extraProjection =
      _.withColumn("vals", org.apache.spark.sql.avro.functions.from_avro(col("value"), testOnAvro.toString)).
        select("vals.*")).instance

    val ro = map(avroTestData, processor)
    ro.map(_.overallResult) shouldBe Seq(Passed, Passed, Passed, Failed, Passed, Failed)
  } } } }

  test("via ProcessFactory map's") { not2_4 { not_Cluster { evalCodeGensNoResolve {
    import sparkSession.implicits._

    val theMap = Seq((40, true),
      (50, false),
      (60, true)
    )
    val lookups = mapLookupsFromDFs(Map(
      "subcodes" -> ( () => {
        val df = theMap.toDF("subcode", "isvalid")
        (df, column("subcode"), column("isvalid"))
      } )
    ), LocalBroadcast(_))

    registerMapLookupsAndFunction(lookups)

    val rs = RuleSuite(Id(1,1), Seq(
      RuleSet(Id(50, 1), Seq(
        Rule(Id(100, 1), ExpressionRule("if(product like '%otc%', account = '4201', mapLookup('subcodes', subcode))"))
      ))
    ))

    val processor = ProcessFunctions.dqFactory[TestOn](rs, inCodegen).instance

    val rc = map(testData, processor)
    rc.map(_.overallResult)  shouldBe Seq(Failed, Passed, Failed, Passed, Passed, Failed)
  } } } }

}
