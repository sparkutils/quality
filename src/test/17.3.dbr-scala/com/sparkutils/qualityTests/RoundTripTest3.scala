package com.sparkutils.qualityTests

import org.apache.spark.sql.functions._
import org.junit.Test
import org.scalatest.FunSuite

class RoundTripTest3 extends FunSuite with RowTools with TestUtils {

  @Test
  def testULEquals: Unit = evalCodeGensNoResolve {
    import sparkSession.implicits._
    val leftRaw =
      for{ i <- 1 to 20 } yield TestIdLeft(i, i)
    val lefts = leftRaw.toDS()

    val rightRaw =
      for{ i <- 1 to 20 by 2 } yield TestIdRight(i, i)
    val rights = rightRaw.toDS()

    val joined = lefts.join(rights, expr("longPairEqual('left', 'right')"))

    import scala.collection.JavaConverters._
    val ids = joined.select("left_higher","left_lower").as[TestIdLeft].toLocalIterator().asScala.map(_.left_lower).toSet
    assert((1 to 20 by 2).toSet == ids)

    // test filesystem and pushed filters
    lefts.write.mode("overwrite").parquet(outputDir+"/lefts")
    rights.write.mode("overwrite").parquet(outputDir+"/rights")

    val flefts = sparkSession.read.parquet(outputDir+"/lefts")
    val frights = sparkSession.read.parquet(outputDir+"/rights")

    val fjoined = flefts.join(frights, expr("longPairEqual('left', 'right')"))
    fjoined.queryExecution.executedPlan.children.foreach{
      p =>
        p.children.foreach { f =>
          val fstring = f.verboseString(1)
          if (fstring.contains("PushedFilters")) {
            assert(fstring.contains("IsNotNull(right_lower)"))
            assert(fstring.contains("IsNotNull(right_higher)"))
          }
        }
    }
  }
}