package com.sparkutils.qualityTests

import com.sparkutils.quality._
import com.sparkutils.quality.functions.{flatten_rule_results, unpack_id_triple}
import com.sparkutils.quality.impl.extension.FunNRewrite
import com.sparkutils.quality.impl.{RuleEngineRunner, RunOnPassProcessor}
import com.sparkutils.shim.expressions.PredicateHelperPlus
import org.apache.spark.sql.{DataFrame, Dataset, QualitySparkUtils, Row, SparkSession}
import org.apache.spark.sql.ShimUtils.{expression, isPrimitive}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.{Expression, MutableProjection, Projection}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, DataType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.junit.Test
import org.junit.runner.RunWith
import org.scalatest.{FunSuite, Matchers}
import org.scalatestplus.junit.JUnitRunner

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

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

  def map(seq: Seq[TestOn], projection: Projection, resi: Int): Seq[Int] = seq.map{ s =>
    val i = InternalRow(UTF8String.fromString(s.product), UTF8String.fromString(s.account), s.subcode)
    projection.asInstanceOf[MutableProjection].target(InternalRow(null, null, null, null, null))
    val r = projection(i)
    r.getInt(resi)
  }

  test("simple projection") {
    val s = sparkSession // force it
    registerQualityFunctions()

    val rs = RuleSuite(Id(1,1), Seq(
      RuleSet(Id(50, 1), Seq(
        Rule(Id(100, 1), ExpressionRule("if(product like '%otc%', account = '4201', subcode = 50)"))
      ))
    ))

    val exprs = QualitySparkUtils.resolveExpressions(typ, taddOverallResultsAndDetailsF(rs))
    val iprocessor = QualitySparkUtils.rowProcessor(exprs, false)
    val cprocessor = QualitySparkUtils.rowProcessor(exprs, true)
    val ro = map(testData, iprocessor, exprs.length - 2)
    ro shouldBe Seq(PassedInt, PassedInt, PassedInt, FailedInt, PassedInt, FailedInt)
    val rc = map(testData, cprocessor, exprs.length - 2)
    rc shouldBe Seq(PassedInt, PassedInt, PassedInt, FailedInt, PassedInt, FailedInt)
  }
}
