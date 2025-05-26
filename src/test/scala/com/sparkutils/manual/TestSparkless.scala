package com.sparkutils.manual

import com.sparkutils.manual.ProcessorThroughputBenchmark.genRules
import com.sparkutils.quality.sparkless.{ProcessFunctions, Processor}
import com.sparkutils.quality.{ExpressionRule, Id, Rule, RuleSet, RuleSuite, RuleSuiteResult}
import com.sparkutils.qualityTests.ResultHelper.longSchema
import com.sparkutils.qualityTests.TestOn
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Row, SQLContext, SQLImplicits, ShimUtils, SparkSession}

/**
 * All the tests rely on a spark session, so this is not automatable and would stop testShades from running
 */
object TestSparkless {

  val testData=Seq(
    TestOn("edt", "4251", 50),
    TestOn("otc", "4201", 40),
    TestOn("fi", "4251", 50),
    TestOn("fx", "4206", 60),
    TestOn("fxotc", "4201", 40),
    TestOn("eqotc", "4200", 60)
  )

  def main(args: Array[String]): Unit = {
    import com.sparkutils.quality.sparkless.ProcessFunctions._

    val sparkSession = SparkSession.builder().
      config("spark.master", s"local[1]").
      config("spark.ui.enabled", false).getOrCreate()

    import sparkSession._

    val ruleSuite = RuleSuite(Id(1, 1), Seq(
      RuleSet(Id(50, 1), Seq(
        Rule(Id(100, 1), ExpressionRule("if(product like '%otc%', account = '4201', subcode = 50)"))
      ))
    ))
    import implicits._
    // thread safe to share
    //val processorFactory = dqFactory[TestOn](ruleSuite)

    val processor = (rules: Int, cols: Int) => {
      implicit val renc = ShimUtils.rowEncoder(longSchema(cols, LongType))
      ProcessFunctions.dqFactory[Row](genRules(rules, cols)//,
      //  forceMutable = true
        ,forceVarCompilation = true
      ).instance
    }
    def startup[T](processor: (Int, Int) => Processor[Row, T])(params: (Int, Int)) = {
      processor(params._1, params._2)
    }
/*
    println("|rulesetCount|fieldCount|actual number of rules|")
    println("|-|-|-|")
    for{
      rules <- 25 to 150 by 25
      fields <- 10 to 50 by 10
    } {
      println(s"| $rules | $fields | ${genRules(rules, fields).ruleSets.flatMap(_.rules).size} |")
    }
*/
    val r = startup(processor)(100,90)

/*
    object implicits extends SQLImplicits with Serializable {
      protected override def _sqlContext: SQLContext = ???
    }*/

    sparkSession.stop()
    // give it a chance to stop
    Thread.sleep(4000)
/*
    try {

      // in other threads an instance is needed
      val threadSpecificProcessor = processorFactory.instance
      try {
        val dqResults: RuleSuiteResult = threadSpecificProcessor(testData.head)
        println(s"""
          dqResults.overallResult from was ${dqResults.overallResult}
          """)
      } finally {
        // when your thread is finished doing work close the instance
        threadSpecificProcessor.close()
      }

    } finally {

    }*/

  }
}
