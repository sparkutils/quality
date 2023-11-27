package com.sparkutils.quality.impl.util

import org.slf4j.LoggerFactory

import scala.util.parsing.combinator.{JavaTokenParsers, PackratParsers}

sealed trait LambdaDocsToken

case class ParamDoc(parameterName: String, docs: String) extends LambdaDocsToken
case class Description(str: String) extends LambdaDocsToken
case class ReturnDoc(str: String) extends LambdaDocsToken

/**
 * Parser which adds token based string reading
 */
trait NotATokenParser extends JavaTokenParsers {
  def breakingTokens: Set[String]

  lazy val notAToken =
    new Parser[String] {
      def apply(in: Input) = {
        val source = in.source
        val offset = in.offset
        val start = handleWhiteSpace(source, offset)
        var j = start
        var break = 0
        var fromStart = false
        while (j <= source.length && break == 0) {
          val breakOption =
            breakingTokens.find { token =>
              val len = j - token.length
              if (len > start) {
                val comp = source.subSequence(j - token.length, j)

                fromStart = false
                token == comp
              } else
                if (j + token.length < source.length) {
                  val comp = source.subSequence(j, j + token.length)

                  fromStart = true
                  token == comp
                } else
                  false

            }
          break = breakOption.fold(0)(_.length)

          if (break == 0) {
            j += 1
          }
        }

        if (break > 0 && fromStart)
          Failure("breakingToken found at at start of token parsing", in.drop(start - offset))
        else
          if (break > 0)
            Success(source.subSequence(start, j - break).toString, in.drop((j - break) - offset))
          else
            Success(source.subSequence(start, j).toString, in.drop(j - offset))
      }
    }
}

/**
 * All params are optional
 * @param description
 * @param params
 * @param returnDescription
 */
case class Docs(description: String = "", params: Map[String, String] = Map.empty, returnDescription: String = "")

/**
 * Simple holder for items within a RuleSuite
 * @param t
 * @param docs
 * @tparam T
 */
case class WithDocs[T](t: T, docs: Docs)

/**
 * Parser for documentation on string expressions, with support for lambdas
 */
object DocsParser extends NotATokenParser with PackratParsers {
  val log = LoggerFactory.getLogger("DocParser")

  val breakingTokens = Set("*/", "@param", "@return")

  lazy val docs = "/**" ~> opt(description) ~ opt(params) ~ opt(ret) ~ ("*/")

  lazy val description = notAToken ^^ (f => Description(f.trim))
  lazy val params = rep(param)
  lazy val param =
    "@param" ~> (ident ~ opt(notAToken)) ^^ {f =>
      ParamDoc(f._1, f._2.map(_.trim).getOrElse(""))
    }

  lazy val ret = "@return" ~> opt(notAToken) ^^ (f => ReturnDoc(f.map(_.trim).getOrElse("")))

  /**
   * If there is a leading doc then it is removed
   * @param s
   * @return
   */
  def stripComments( s: String ): String = {
    val ret = parseAll(phrase(docs), s)
    ret match {
      case Success(result, nextInput) =>
        result._2

      case NoSuccess(msg, nextInput) =>
        s
    }
  }

  /**
   * Removes any leading tabs/spaces or *'s from the string and any leading characters before the start of the docs
   * @param str
   * @return
   */
  def cleanDocs(str: String): String = {
    val pos = str.indexOf("/**")
    val epos = str.indexOf("*/")
    if (pos > -1 && epos > -1) {
      val docstart = str.substring(pos, epos + 2)

      val lines = docstart.split("\n")
      val r = "^\\s+\\*".r
      val endr = "^\\s+\\*/".r

      // if every one starts with * it's a scaladoc, otherwise it's markdown and leave it alone
      if (lines.tail.forall(s => r.findPrefixOf(s).isDefined)) {
        lines.map {
          case s if endr.findPrefixOf(s).isDefined => s
          case s => s.replaceAll("^\\s+\\*", "")
        }.mkString("\n")
      } else
        str // single line in the original
    } else
      str // not a doc
  }

  /**
   * Parses sql with a leading docs
   * @param s
   * @return Some(Docs) when there is a documentation object
   */
  def parse( s: String ): Option[Docs] = {
    val ret = parseAll(phrase(docs), cleanDocs(s) + " ") // trailing space acts a termination char
    ret match {
      case Success(result, nextInput) =>
        Some(Docs(result._1._1._1.map(_.str).getOrElse(""),
          result._1._1._2.map(t => t.map( d => d.parameterName -> d.docs).toMap ).getOrElse(Map.empty),
          result._1._2.map(_.str).getOrElse("")
        ))

      case NoSuccess(msg, nextInput) =>
        log.debug(s"DocsParser couldn't pass - $msg")
        None
    }
  }
}
