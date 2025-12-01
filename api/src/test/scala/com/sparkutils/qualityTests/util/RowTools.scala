package com.sparkutils.qualityTests.util

import com.sparkutils.quality._
import ResultHelper.longSchema
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, ShimUtils}
import org.scalameter.Gen.crossProduct
import org.scalameter.api.Gen

trait RowTools extends TestUtilsBase {

  val structWithColumnExpr = (rules: Int, cols: Int, df: DataFrame) =>
    df.withColumn("DataQuality", ruleRunner(genRules(rules, cols), compileEvals = false))

  val structWithColumnExprEvalCompiled = (rules: Int, cols: Int, df: DataFrame) =>
    df.withColumn("DataQuality", ruleRunner(genRules(rules, cols), compileEvals = true))

  val structWithColumnExprButRunnerEval = (rules: Int, cols: Int, df: DataFrame) =>
    df.withColumn("DataQuality", ruleRunner(genRules(rules, cols), compileEvals = false, forceRunnerEval = true))

  val structWithColumnExprEvalCompiledButRunnerEval = (rules: Int, cols: Int, df: DataFrame) =>
    df.withColumn("DataQuality", ruleRunner(genRules(rules, cols), compileEvals = true, forceRunnerEval = true))

  val noRules = (rules: Int, cols: Int, df: DataFrame) =>
    df.withColumn("DataQuality", lit("place"))

  val simplePassedProbabilityRule = RuleSuite(Id(1,1), Seq(
    RuleSet(Id(50, 1), Seq(
      Rule(Id(100, 1), ExpressionRule("0.9"))
    ))
  ))

  val simpleFailedProbabilityRule = RuleSuite(Id(1,1), Seq(
    RuleSet(Id(50, 1), Seq(
      Rule(Id(100, 1), ExpressionRule("0.6"))
    ))
  ))

  def genRules(rules: Int, cols: Int) =
    RuleSuite(Id(1, 1),
      for{
        r <- 2 to rules by 5
      } yield
        RuleSet(Id(50+r, 1),
          for {
            c <- 0 to cols / 2
          } yield Rule(Id(100+c, 1),
            ExpressionRule(s"(`$c` % 2) = 0")
          )
        )
    )

  import scala.collection.JavaConverters._

//  val fields = Gen.range("fieldCount")(27, 30, 5) // 1500 gives gc issues, takes forever to warm up, probably will soe
//  val ruleSets = Gen.range("rulesetCount")(27,30, 5) // number of rule sets run  2, 1)//
  val fields = Gen.range("fieldCount")(10, 50, 10) // 800 gives 1500 gives gc issues, takes forever to warm up, probably will soe
  val ruleSets = Gen.range("rulesetCount")(25, 150, 25) // number of rule sets run  2, 1)//

  val generator = crossProduct(ruleSets, fields)

  //val writeRows = 100000
  val writeRows = 1000
  //val writeRows = 10000

  // data grows with the rows...
  def sampleDataAsLong[T](maxRows: Int, maxCols: Int, startValue: T): Seq[Row] =
    (0 to maxRows).map(i => (0L until maxCols).map(i+_)).map(t => Row((t :+ startValue) :_*))

  def dataFrameLong[T](maxRows: Int, maxCols: Int, dataType: DataType, startValue: T) = sparkSession.createDataFrame(sampleDataAsLong(maxRows, maxCols, startValue).asJava, longSchema(maxCols, dataType))

  def sampleDataAsLongLazy[T](ids: Dataset[java.lang.Long], maxCols: Int, startValue: T, structType: StructType): DataFrame = {
    implicit val renc = ShimUtils.rowEncoder(structType)
    ids.map(i => Row(((0L until maxCols).map(i+_) :+ startValue) :_*))
  }

  /**
   * unlike dataFrameLong it's lazy
   */
  def dataFrameLongLazy[T](maxRows: Int, maxCols: Int, dataType: DataType, startValue: T) = sampleDataAsLongLazy(sparkSession.range(0, maxRows), maxCols, startValue, longSchema(maxCols, dataType))

}
