package com.sparkutils.qualityTests

import com.sparkutils.quality._
import com.sparkutils.quality.impl.LambdaFunctionImpl
import com.sparkutils.qualityTests.util.SharedConnectTests
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
