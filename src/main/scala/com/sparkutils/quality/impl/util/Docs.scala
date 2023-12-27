package com.sparkutils.quality.impl.util

import com.sparkutils.quality.impl.{DataFrameSyntaxError, ExpressionLookup, HasId, HasNonIdText, HasOutputText, HasRuleText, LambdaMultipleImplementationWithSameArityError, LambdaRelevant, NoOpRunOnPassProcessor, NonLambdaDocParameters, OutputExpressionRelevant, RuleError, RuleRegistrationFunctions, RuleRelevant, RuleWarning, RunOnPassProcessor}
import com.sparkutils.quality.{Id, Rule, RuleSuite, RunOnPassProcessor}

import scala.util.parsing.combinator.{JavaTokenParsers, PackratParsers}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import java.net.URI

/**
 * All identified docs, or empty Docs, for each RuleSuite expression type
 * @param rules
 * @param outputExpressions
 * @param lambdas
 */
case class RuleSuiteDocs(rules: Map[Id, WithDocs[Rule]] = Map.empty,
                         outputExpressions: Map[Id, WithDocs[RunOnPassProcessor]] = Map.empty,
                         lambdas: Map[Id, WithDocs[com.sparkutils.quality.impl.LambdaFunction]] = Map.empty)

object RuleSuiteDocs {
  type IdTrEither = TrEither[Id, Id, Id]
  def LambdaId(id: Id): IdTrEither = Tr1(id)
  def OutputExpressionId(id: Id): IdTrEither = Tr2(id)
  def RuleId(id: Id): IdTrEither = Tr3(id)

  protected def genRule(any: AnyRef, id: IdTrEither, expressionLookups: Map[IdTrEither, ExpressionLookup], idGen: (String, Id) => String, rsd: RuleSuiteDocs, extraFunctionListClass: String, qualityDocLink: String) =
    if (any.isInstanceOf[HasRuleText])
      s"""
```sql
${DocsParser.stripComments(any.asInstanceOf[HasRuleText].rule)}
```
${
        expressionLookups.get(id).fold("") { expr =>
          val qf = genLinks(expr.sparkFunctions.filter(RuleRegistrationFunctions.qualityFunctions), "Quality functions used", "spark_functions_used", extraFunctionListClass) {
            fname =>
              s"""<a target="_blank" href="$qualityDocLink#${fname.toLowerCase}">$fname</a>"""
          }
          val sf = genLinks(expr.sparkFunctions.filterNot(RuleRegistrationFunctions.qualityFunctions), "Spark functions used", "spark_functions_used", extraFunctionListClass) {
            fname =>
              s"""<a target="_blank" href="https://spark.apache.org/docs/latest/api/sql/index.html#$fname">$fname</a>"""
          }
          val lm = genLinks(expr.lambdas, "Lambda used", "lambdas_used", extraFunctionListClass) {
            lid => s"""<a href="#${idGen("lambda", lid)}">${rsd.lambdas(lid).t.name}</a>"""
          }
          qf + sf + lm
        }
      }
"""
    else ""

  /**
   * used by both outputexpressions and lambdas
   *
   * @param id
   * @param reverseLookups id to a full set of reference id's, Left is lambda Right is OutputExpression
   */
  protected def genBackLinks(id: IdTrEither, reverseLookups: Map[IdTrEither, Set[IdTrEither]], docs: RuleSuiteDocs, extraFunctionListClass: String, idGen: (String, Id) => String): String =
    reverseLookups.get(id).fold("") { ids =>
      val lambdas = docs.lambdas.filterKeys(k => ids.contains(LambdaId(k))).keys
      val output = docs.outputExpressions.filterKeys(k => ids.contains(OutputExpressionId(k))).keys
      val rules = docs.rules.filterKeys(k => ids.contains(RuleId(k))).keys

      val ls = genLinks(lambdas, "Called by Lambdas", "lambdas_used", extraFunctionListClass) {
        lid => s"""<a href="#${idGen("lambda", lid)}">${docs.lambdas(lid).t.name}</a>"""
      }
      val os = genLinks(output, "Called by Output Expressions", "lambdas_used", extraFunctionListClass) {
        oid => s"""<a href="#${idGen("outputRule", oid)}">${oid.id} - ${oid.version}</a>"""
      }
      val rs = genLinks(rules, "Called by Rules", "lambdas_used", extraFunctionListClass) {
        rid => s"""<a href="#${idGen("rule", rid)}">${rid.id} - ${rid.version}</a>"""
      }

      ls + os + rs
    }

  protected def genLinks[T](theTs: Iterable[T], title: String, flclass: String, extraFunctionListClass: String)
                           (f: T => String) =
    if (theTs.nonEmpty) {
      s"""
<div class="$flclass $extraFunctionListClass">
<p>$title:</p>
<ul>
${
        theTs.map(what =>
          s"""
<li> ${f(what)} </li>""").mkString("\n")
      }
</ul>
</div>
"""
    } else ""

  protected def genRuleNoStripping(any: AnyRef) =
    if (any.isInstanceOf[HasRuleText])
      s"""
```sql
${any.asInstanceOf[HasRuleText].rule}
```
"""
    else ""

  /**
   * Configures local lookup when there are errors or warnings
   *
   * @param relativePath the path, typically a relative .md filename
   * @param errors
   * @param warnings
   * @param errorClass   this class will wrap the anchor to any errors
   * @param warningClass this class will wrap the anchor to any warnings
   */
  case class RelativeWarningsAndErrors(relativePath: String, errors: Set[RuleError], warnings: Set[RuleWarning], errorClass: String = "rule_error", warningClass: String = "rule_warning") {
    val groupedErrors = errors.groupBy(_.id)
    val groupedWarnings = warnings.groupBy(_.id)
  }

  protected def genRelativeWarningsAndErrors(id: Id, relativeWarningsAndErrors: RelativeWarningsAndErrors, idGen: (String, Id) => String) =
    s"""${ // attempt both errors and warnings
      if (relativeWarningsAndErrors.groupedErrors.contains(id))
        s""" <a href="${relativeWarningsAndErrors.relativePath}#${idGen("Errors", id)}" class="${relativeWarningsAndErrors.errorClass}">${relativeWarningsAndErrors.groupedErrors(id).size} Errors</a>"""
      else
        ""
    }${
      if (relativeWarningsAndErrors.groupedWarnings.contains(id))
        s""" <a href="${relativeWarningsAndErrors.relativePath}#${idGen("Warnings", id)}" class="${relativeWarningsAndErrors.warningClass}">${relativeWarningsAndErrors.groupedWarnings(id).size} Warnings</a>"""
      else
        ""
    }"""

  /**
   * Creates a markdown document without H1.
   *
   * @param rsd                      result of calling validate on a ruleSuite
   * @param ruleSuite                the ruleSuite, used to group RuleSets to rules
   * @param expressionLookups        identified function and lambda usage
   * @param relativeErrorsAndWarning if it's empty then no errors and warnings will be linked, providing a relative md will cause two links to be present when any are identified
   * @param idGen                    allows overriding the href name for a rule id (primarily for output expressions)
   */
  // TODO add a callback for annotations providing the expression text id?
  def createMarkdown(rsd: RuleSuiteDocs, ruleSuite: RuleSuite, expressionLookups: Map[IdTrEither, ExpressionLookup], qualityDocLink: String, relativeErrorsAndWarning: Option[RelativeWarningsAndErrors] = None, idGen: (String, Id) => String = (typeString, id: Id) => new URI(s"${typeString}_${id.id}_${id.version}").toASCIIString, extraFunctionListClass: String = "comma-list"): String = {

    // left is lambda, right is output
    val reverseLookups: Map[IdTrEither, Set[IdTrEither]] = {
      val trl = mutable.Map.empty[IdTrEither, Set[IdTrEither]]

      rsd.lambdas.foreach { lambda =>
        var set = trl.getOrElse(LambdaId(lambda._1), Set.empty[IdTrEither])

        expressionLookups.foreach { ep =>
          val el = ep._2
          if (el.lambdas.contains(lambda._1)) {
            set += ep._1
          }
        }

        trl(LambdaId(lambda._1)) = set
      }
      rsd.rules.foreach { pair =>
        if (pair._2.t.runOnPassProcessor ne NoOpRunOnPassProcessor.noOp) {
          val outid = pair._2.t.runOnPassProcessor.id
          var set = trl.getOrElse(OutputExpressionId(outid), Set.empty[IdTrEither])
          set += RuleId(pair._1)
          trl(OutputExpressionId(outid)) = set
        }
      }

      Map() ++ trl
    }

    def genDocs(docs: Docs) =
      s"""${docs.description}
${
        if (docs.params.nonEmpty)
          s"""
|Parameter|Description|
|---|---|
${docs.params.map(pair => s"|${pair._1}|${pair._2}|").mkString("\n")}
""" else ""
      }

${if (docs.returnDescription.nonEmpty) s"__Returns__: ${docs.returnDescription}" else ""}
    """

    def rule(id: Id) =
      rsd.rules.get(id).map { withDocs =>
        val WithDocs(rule, docs) = withDocs
        val id = rule.id
        s"""
#### Rule Id - ${id.id}, ${id.version} <a name="${idGen("rule", id)}"></a> - ${relativeErrorsAndWarning.map(r => genRelativeWarningsAndErrors(id, r, idGen)).getOrElse("")}
${genDocs(docs)}
${genRule(rule.expression, RuleId(id), expressionLookups, idGen, rsd, extraFunctionListClass, qualityDocLink)}
${
          if (rule.runOnPassProcessor ne NoOpRunOnPassProcessor.noOp)
            s"""
__Triggers__ output rule with id <a href="#${idGen("outputRule", rule.runOnPassProcessor.id)}">${rule.runOnPassProcessor.id.id}, ${rule.runOnPassProcessor.id.version}</a> _Salience_ ${rule.runOnPassProcessor.salience}
""" else ""
        } """
      }.getOrElse("")

    def rules =
      s"""
${
        ruleSuite.ruleSets.map { rs =>
          s"""
### RuleSet Id - ${rs.id.id}, ${rs.id.version} <a name="${idGen("ruleSet", rs.id)}"></a>
${
            rs.rules.map { r =>
              rule(r.id)
            }.mkString("")
          }
"""
        }.mkString("")
      }
"""

    def outputRules =
      rsd.outputExpressions.map { pair =>
        val (id, WithDocs(rule, docs)) = pair
        s"""
### Output Rule Id - ${id.id}, ${id.version} <a name="${idGen("outputRule", id)}"></a> - ${relativeErrorsAndWarning.map(r => genRelativeWarningsAndErrors(id, r, idGen)).getOrElse("")}
${genDocs(docs)}
${genRule(rule.returnIfPassed, OutputExpressionId(id), expressionLookups, idGen, rsd, extraFunctionListClass, qualityDocLink)}
${genBackLinks(OutputExpressionId(id), reverseLookups, rsd, extraFunctionListClass, idGen)}
"""
      }.mkString("\n")

    def lambdas =
      rsd.lambdas.groupBy(pair => pair._2.t.name).map { namepair =>
        s"""
## Lambda ${namepair._1}
    ${
          namepair._2.map { pair =>
            val (id, WithDocs(rule, docs)) = pair
            s"""
### Rule - Id - ${id.id}, ${id.version} <a name="${idGen("lambda", id)}"></a> - ${relativeErrorsAndWarning.map(r => genRelativeWarningsAndErrors(id, r, idGen)).getOrElse("")}
__Name__ ${rule.name}
${genDocs(docs)}
${genRule(rule, LambdaId(id), expressionLookups, idGen, rsd, extraFunctionListClass, qualityDocLink)}
${genBackLinks(LambdaId(id), reverseLookups, rsd, extraFunctionListClass, idGen)}
"""
          }.mkString("\n")
        }
"""
      }.mkString("\n")

    s"""
## RuleSuite Id ${ruleSuite.id.id}, ${ruleSuite.id.version} <a name="${idGen("ruleSuite", ruleSuite.id)}"></a> - ${relativeErrorsAndWarning.map(r => s"""<span class="${r.errorClass}">${r.errors.size} Errors</span><span class="${r.warningClass}"> ${r.warnings.size} Warnings</span>""").getOrElse("")}

$rules

## Output Rules

$outputRules

## Lambdas

$lambdas

"""
  }

  protected def showFun[T, R <: HasId with HasOutputText](ruleError: R, map: Map[Id, WithDocs[T]], anchorId: String, relativePathToDocs: String)(ruleToHasText: T => AnyRef) = {
    val errorText = ruleError.outputText
    val id = ruleError.id
    s"""
__${ruleError.getClass.getSimpleName}__ ${errorText} against ${exprText(id, map, anchorId, relativePathToDocs)(ruleToHasText)}
"""
  }

  protected def exprText[T](id: Id, map: Map[Id, WithDocs[T]], anchorId: String, relativePathToDocs: String)(ruleToHasText: T => AnyRef) =
    s"""<a href="$relativePathToDocs#$anchorId">expression</a>
${map.get(id).map(docs => genRuleNoStripping(ruleToHasText(docs.t))).getOrElse("")}"""

  protected def showAll[R <: HasId with HasOutputText](header: String, errors: Map[Id, Set[R]], ruleSuite: RuleSuite, ruleSuiteDocs: RuleSuiteDocs, relativePathToDocs: String, idGen: (String, Id) => String = (typeString, id: Id) => new URI(s"${typeString}_${id.id}_${id.version}").toASCIIString) = {
    def ruleFun[R <: HasId with HasOutputText](e: R) = showFun(e, ruleSuiteDocs.rules, idGen("rule", e.id), relativePathToDocs)(_.expression)
    def outputFun[R <: HasId with HasOutputText](e: R) = showFun(e, ruleSuiteDocs.outputExpressions, idGen("outputExpression", e.id), relativePathToDocs)(_.returnIfPassed)

    if (errors.nonEmpty)
      s"""
## $header Identified for RuleSuite - Id ${ruleSuite.id.id}, ${ruleSuite.id.version} <a name="${idGen(s"ruleSuite$header", ruleSuite.id)}"></a>
${
        errors.map { pair =>
          s"""
### Id ${pair._1.id}, ${pair._1.version} <a name="${idGen(s"$header", ruleSuite.id)}"></a>
${
            pair._2.map {
              case e@LambdaMultipleImplementationWithSameArityError(name, count, argLength, ids) =>
                s"""
__LambdaMultipleImplementationWithSameArityError__: ${e.errorText}
_Against_
${
                  val map = ruleSuiteDocs.lambdas
                  ids.map(id => exprText(id, ruleSuiteDocs.lambdas, idGen("lambda", e.id), relativePathToDocs)(_)).mkString("\n")
                }
"""
              case e: LambdaRelevant => showFun(e, ruleSuiteDocs.lambdas, idGen("lambda", e.id), relativePathToDocs)(identity)
              case e: RuleRelevant => ruleFun(e)
              case e: OutputExpressionRelevant => outputFun(e)
              // either output or lambda possible
              case e@NonLambdaDocParameters(id) if ruleSuiteDocs.outputExpressions.contains(id) =>
                outputFun(e)
              case e@NonLambdaDocParameters(id) if ruleSuiteDocs.rules.contains(id) =>
                ruleFun(e)
              case DataFrameSyntaxError(err) => s"__DataFrameSyntaxError__: Processing the sample DataFrame produced the following error $err"
            }.mkString("\n")
          }"""
        }.mkString("\n")
      }
"""
    else ""
  }

  def createErrorAndWarningMarkdown(ruleSuiteDocs: RuleSuiteDocs, ruleSuite: RuleSuite, relative: RelativeWarningsAndErrors, idGen: (String, Id) => String = (typeString, id: Id) => new URI(s"${typeString}_${id.id}_${id.version}").toASCIIString): String = {

    def showSummaryFor[T, R <: HasNonIdText](ruleErrors: Set[R], summaryType: String) = {
      val grouped = ruleErrors.groupBy(e => s"__${e.getClass.getSimpleName}__ ${e.nonIdText}")
      if (ruleErrors.isEmpty)
        "" else
        s"""
## $summaryType Summary
|Type|Count|
|---|---:|
      ${
          grouped.map(p =>
            s"""| ${p._1} | ${p._2.size} |""").mkString("\n")
        }"""
    }

    s"""
${showSummaryFor(relative.errors, "Errors")}
${showSummaryFor(relative.warnings, "Warnings")}
${showAll("Errors", relative.groupedErrors, ruleSuite, ruleSuiteDocs, relative.relativePath, idGen)}
${showAll("Warnings", relative.groupedWarnings, ruleSuite, ruleSuiteDocs, relative.relativePath, idGen)}
"""
  }
}
