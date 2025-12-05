package com.sparkutils.qualityTests.classicOnly

import com.sparkutils.quality.classicFunctions.registerQualityFunctions
import com.sparkutils.quality.impl.id.{GenericLongBasedIDExpression, model}
import com.sparkutils.quality.impl.rng.RandomLongs
import com.sparkutils.qualityTests.VariableTestShims
import com.sparkutils.qualityTests.util.ClassicSharedTests
import com.sparkutils.testing.TestUtils.debug
import org.apache.commons.rng.simple.RandomSource
import org.apache.spark.sql.{DataFrame, ShimUtils}

class ClassicIDTests extends ClassicSharedTests with VariableTestShims {

  import org.apache.spark.sql.ShimUtils.expression


  test("testRNGIDGenNonJump") {
    classicOnly {
      evalCodeGensNoResolve {
        registerQualityFunctions()

        def testRes(rngExploded: DataFrame): Unit = {
          debug(rngExploded.show())
          assert(rngExploded.schema.fields.map(_.name).toSeq
            == Seq("id", "rng_id_base", "rng_id_i0", "rng_id_i1"), "Column names incorrect")
        }

        def nonJump(prefix: String) =
          ShimUtils.column(GenericLongBasedIDExpression(model.RandomID,
            expression(RandomLongs(RandomSource.KISS)), prefix))

        val df = sparkSession.range(0, idRange)
        val rngExploded = df.withColumn("rng_id", nonJump("rng_id")).selectExpr("id", "rng_id.*")
        testRes(rngExploded)
      }
    }
  }
}
