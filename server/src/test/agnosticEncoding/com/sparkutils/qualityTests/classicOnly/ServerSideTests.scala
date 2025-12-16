package com.sparkutils.qualityTests.classicOnly

import com.sparkutils.quality.QualityException
import com.sparkutils.quality.impl.OfRuleSuite
import com.sparkutils.qualityTests.util.ClassicSharedTests
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.BinaryType
import org.scalatest.Matchers

class ServerSideTests extends ClassicSharedTests with Matchers {

  test("null shouldn't match") {
    null match {
      case OfRuleSuite(_) => fail("Should not have matched")
      case _ => ()
    }
  }

  test("literal of string shouldn't match") {
    Literal("str") match {
      case OfRuleSuite(_) => fail("Should not have matched")
      case _ => ()
    }
  }

  test("bad bytes should throw") {
    val caught =
      intercept[QualityException] { // Result type: IndexOutOfBoundsException


      Literal(Array.ofDim[Byte](22), BinaryType) match {
        case OfRuleSuite(_) => fail("Should not have matched")
        case _ => ()
      }

    }

    caught.msg should include("Could not deserialize")
  }
}
