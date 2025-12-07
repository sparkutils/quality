package com.sparkutils.qualityTests.classicOnly

import com.sparkutils.quality.impl.util.{Docs, DocsParser}
import org.scalatest.{FunSuite, Matchers}

class DocsParserTest extends FunSuite with Matchers {

  test("simpleParsingTest") {
    val test = "/** My Description @param name name desc @param othername othername desc @return return val*/ "

    val res = DocsParser.parse(test)

    assert(res.contains(
      Docs("My Description", Map("name" -> "name desc", "othername" -> "othername desc"), "return val")
    ))
  }

  test("leadingWhiteSpacesTest") {
    val test = " \n\r\n\t  /** My Description @param name name desc @param othername othername desc @return return val*/ "

    val res = DocsParser.parse(test)

    assert(res.contains(
      Docs("My Description", Map("name" -> "name desc", "othername" -> "othername desc"), "return val")
    ))
  }

  test("trailingWhiteSpacesWithAnnotationsTest") {
    val test = "/** My Description @param name name desc @param othername othername desc @return return val*/  \n\r\n\t  @location"

    val res = DocsParser.parse(test)

    assert(res.contains(
      Docs("My Description", Map("name" -> "name desc", "othername" -> "othername desc"), "return val")
    ))
  }

  test("multilineStarsOnLinesTest") {
    val test =
      """
         /**
          * My Description
          * @param name name desc
          * @param othername othername desc
          * @return return val
          */"""

    val res = DocsParser.parse(test)

    res should contain(
      Docs("My Description", Map("name" -> "name desc", "othername" -> "othername desc"), "return val")
    )
  }

  test("emptyDescReturnStarsOnLinesTest") {
    val test =
      """
       /**
        * My Description
        * @param name name desc
        * @param othername othername desc
        * @return
        */"""

    val res = DocsParser.parse(test)

    res should contain(
      Docs("My Description", Map("name" -> "name desc", "othername" -> "othername desc"), "")
    )
  }

  test("emptyDescParamStarsOnLinesTest") {
    val test =
      """
     /**
      * My Description
      * @param name
      * @param othername othername desc
      * @return return val
      */"""

    val res = DocsParser.parse(test)

    res should contain(
      Docs("My Description", Map("name" -> "", "othername" -> "othername desc"), "return val")
    )
  }

  test("simpleNoParamsTest") {
    val test = "/** My Description @return return val*/ "

    val res = DocsParser.parse(test)

    assert(res.contains(
      Docs("My Description", Map.empty, "return val")
    ))
  }

  test("simpleNoReturnTest") {
    val test = "/** My Description @param name name desc @param othername othername desc */ "

    val res = DocsParser.parse(test)

    assert(res.contains(
      Docs("My Description", Map("name" -> "name desc", "othername" -> "othername desc"))
    ))
  }

  test("simpleDescOnlyTest") {
    val test = "/** My Description */ "

    val res = DocsParser.parse(test)

    assert(res.contains(
      Docs("My Description")
    ))
  }

  test("descOnlyButWithExprTest") {
    val test = "/** My Description */ var -> var + 1"

    val res = DocsParser.parse(test)

    assert(res.contains(
      Docs("My Description")
    ))
  }

  test("noDocsTest") {
    val test = "var -> var + 1"

    val res = DocsParser.parse(test)

    assert(res.isEmpty)
  }

  test("simpleParamsOnlyTest") {
    val test = "/** @param name name desc @param othername othername desc */ "

    val res = DocsParser.parse(test)

    // empty desc
    assert(res.filter(_.description.isEmpty).isDefined)
  }

  test("markdownParsingTest") {
    val desc = """My Description:

* Bullet Point
* Another

"""
    val param = """
othername desc that

has

paragraphs
"""

    val test =
      s"""
/** $desc
@param name name desc
@param othername $param
@return return val
*/ """.stripMargin

    val res = DocsParser.parse(test)

    assert(res.contains(
      Docs(desc.trim(), Map("name" -> "name desc", "othername" -> param.trim()), "return val")
    ))
  }

}
