package com.sparkutils.qualityTests

import com.sparkutils.quality._
import com.sparkutils.quality.functions._
import com.sparkutils.quality.impl.{LambdaFunctionImpl, YamlDecoder}
import com.sparkutils.quality.impl.extension.QualityFunctionParser.{CREATE_FUNCTION_PREFIX, WITH_TOKEN}
import com.sparkutils.qualityTests.util.SharedConnectTests
import com.sparkutils.testing.{ConnectOnly, ConnectionType, UseBoth}
import org.apache.spark.SparkException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataType
import org.scalatest.Matchers

import scala.language.postfixOps

class RemoteFunctionTests extends SharedConnectTests with Matchers {

  test("single function") {
    val s = sparkSession
    import s.implicits._

    registerLambdaFunctions(Seq(LambdaFunctionImpl( "my_echo", "in -> in", Id(-1,-1))))
    sparkSession.sql("select my_echo('a')").as[String].head() shouldBe "a"
  }

  test("multiple functions with new lines") {
    val s = sparkSession
    import s.implicits._

    registerLambdaFunctions(Seq(LambdaFunctionImpl( "my_echo",
      """in ->
        |
        |
        |in""".stripMargin, Id(-1,-1)),
      LambdaFunctionImpl( "my_echo2",
        """in2 ->
          |
          |
          |
          |
          |in2""".stripMargin, Id(-1,-1)),
      LambdaFunctionImpl( "my_echo3",
        """in3 ->
          |
          |
          |
          |
          |
          |
          |in3""".stripMargin, Id(-1,-1))
    ))
    sparkSession.sql("select my_echo3(my_echo2(my_echo('a')))").as[String].head() shouldBe "a"
  }

}
