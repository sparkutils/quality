package com.sparkutils.qualityTests.classicOnly

import com.sparkutils.quality._
import com.sparkutils.quality.impl.IfRelevantExpr
import com.sparkutils.quality.impl.imports.ClassicRuleResultsImports.IgnoredRuleExpr
import com.sparkutils.quality.impl.imports.RuleResultsImports.IgnoredRuleInt
import com.sparkutils.quality.impl.util.TopLevelBoolean
import com.sparkutils.qualityTests.util.ClassicSharedTests
import com.sparkutils.testing.ConnectionType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, BoundReference, EqualTo, Expression, GreaterThan, If, LessThan, Literal}
import org.apache.spark.sql.types.IntegerType
import org.scalatest.Matchers

/**
 * #148 - top level boolean grouping of if_relevant and if(bool,bool,bool), and grouping with the
 * dq ruleRunner at all, every other grouping test uses ruleEngineRunner.
 *
 * A group is only entered when its filter is true, when false the rules keep the runner's
 * defaultRuleResult.  That is right for And derived filters, where filter false means the
 * conjunction is false so the rule did not trigger, but wrong for if_relevant where filter false
 * has the meaningful answer IgnoredRule, hence Group.filterFalseResults.
 */
class TopLevelBooleanFilterFalseTest extends ClassicSharedTests with Matchers {

  override val runWith: ConnectionType = com.sparkutils.testing.ClassicOnly

  private val bA = BoundReference(0, IntegerType, nullable = false)
  private val bB = BoundReference(1, IntegerType, nullable = false)
  private val filter = GreaterThan(bA, Literal(0))
  private val cond = EqualTo(bB, Literal(1))

  // a = -1 so the filter is false, b = 1 so the cond is true
  private val filterFalseRow = InternalRow(-1, 1)

  test("And groups both top level booleans and needs no filter false result") {
    val e = And(filter, cond)
    TopLevelBoolean.fromParts(e).size shouldBe 2
    TopLevelBoolean.filterFalseResultFor(filter, e) shouldBe None
  }

  test("if_relevant offers its filter and answers IgnoredRule for filter false") {
    val e = IfRelevantExpr(filter, cond)
    e.dataType shouldBe IntegerType
    TopLevelBoolean.fromParts(e) should contain(filter: Expression)
    TopLevelBoolean.filterFalseResultFor(filter, e).map(_.eval(filterFalseRow)) shouldBe Some(IgnoredRuleInt)
  }

  test("if(c,a,b) offers its condition and answers the else branch for filter false") {
    val elseBranch = EqualTo(bB, Literal(99))
    val e = If(filter, cond, elseBranch)
    TopLevelBoolean.fromParts(e) should contain(filter: Expression)
    TopLevelBoolean.filterFalseResultFor(filter, e) shouldBe Some(elseBranch)
  }

  test("a filter false result is only claimed when the group filter is the condition") {
    val unrelated = GreaterThan(bB, Literal(42))
    TopLevelBoolean.filterFalseResultFor(unrelated, IfRelevantExpr(filter, cond)) shouldBe None
  }

  // fromParts recurses into a composite filter and the frequency filter in `from` keeps only the
  // shared conjuncts, so the group filter is regularly one conjunct of the if_relevant filter
  // rather than the whole of it.  A conjunct being false still makes the conjunction false, so
  // IgnoredRule is still the answer -- semanticEquals alone missed this and emitted no else
  // branch, leaving the rule on the runner's defaultRuleResult (Passed for dq).
  test("a conjunct of a composite if_relevant filter still claims IgnoredRule") {
    val other = LessThan(bB, Literal(10))
    val e = IfRelevantExpr(And(filter, other), cond)

    TopLevelBoolean.filterFalseResultFor(filter, e) shouldBe Some(IgnoredRuleExpr)
    TopLevelBoolean.filterFalseResultFor(other, e) shouldBe Some(IgnoredRuleExpr)
    TopLevelBoolean.filterFalseResultFor(And(filter, other), e) shouldBe Some(IgnoredRuleExpr)
    // still nothing for an expression that is not part of the filter at all
    TopLevelBoolean.filterFalseResultFor(GreaterThan(bB, Literal(42)), e) shouldBe None
  }

  test("a conjunct of a composite if condition still claims the else branch") {
    val other = LessThan(bB, Literal(10))
    val elseBranch = EqualTo(bB, Literal(99))
    val e = If(And(filter, other), cond, elseBranch)

    TopLevelBoolean.filterFalseResultFor(filter, e) shouldBe Some(elseBranch)
    TopLevelBoolean.filterFalseResultFor(other, e) shouldBe Some(elseBranch)
    TopLevelBoolean.filterFalseResultFor(GreaterThan(bB, Literal(42)), e) shouldBe None
  }

  // the nullable guard must survive the conjunct widening above
  test("a conjunct of a nullable composite if_relevant filter claims nothing") {
    val nullablePart = GreaterThan(BoundReference(0, IntegerType, nullable = true), Literal(0))
    val e = IfRelevantExpr(And(nullablePart, filter), cond)
    TopLevelBoolean.filterFalseResultFor(nullablePart, e) shouldBe None
    TopLevelBoolean.filterFalseResultFor(filter, e) shouldBe None
  }

  // if_relevant answers Failed for a null filter and IgnoredRule for a false one, the generated
  // branch is `if ((!isNull) && value)` so both go to the else, which can only write one answer
  test("a nullable if_relevant filter is not grouped") {
    val nullable = GreaterThan(BoundReference(0, IntegerType, nullable = true), Literal(0))
    val e = IfRelevantExpr(nullable, cond)
    TopLevelBoolean.fromParts(e) should not contain (nullable: Expression)
    TopLevelBoolean.filterFalseResultFor(nullable, e) shouldBe None
  }

  test("removing the top level alone changes the answer, hence the else branch") {
    val e = IfRelevantExpr(filter, cond)
    val rewritten = TopLevelBoolean.removeTopLevels(Set[Expression](filter), e)
    rewritten should not be e
    e.eval(filterFalseRow) shouldBe IgnoredRuleInt
    rewritten.eval(filterFalseRow) should not be e.eval(filterFalseRow)
  }

  private val ruleCount = 40

  private def suiteOf(expr: Int => String) =
    RuleSuite(Id(10, 2), Seq(RuleSet(Id(20, 1),
      (for { i <- 0 until ruleCount } yield Rule(Id(1000 + i, 1), ExpressionRule(expr(i)))).toSeq)))

  private def runIt(rs: RuleSuite, extra: Map[String, String]): Seq[RuleResult] = {
    import com.sparkutils.quality.implicits._
    val processed = sparkSession.range(0, 100).select(ruleRunner(rs, extraConfig = extra).as("res"))
    val res = processed.selectExpr("res.*").as[RuleSuiteResult].collect().head
    res.ruleSetResults.head._2.ruleResults.toSeq.sortBy(_._1.id).map(_._2)
  }

  private val grouping = Map(
    groupProcessorKey -> topLevelBooleanGrouper,
    groupProcessorPercentFilter -> "0.1"
  )

  /** many rules sharing one filter so the grouper buckets them */
  private def groupedAgreesWithUngrouped(expr: Int => String)(check: Seq[RuleResult] => Unit): Unit =
    not3_0_or_3_1 {
      val rs = suiteOf(expr)
      val ungrouped = runIt(rs, Map.empty)
      ungrouped should have size ruleCount
      check(ungrouped)
      runIt(rs, grouping) shouldBe ungrouped
    }

  test("grouped And derived rules agree with ungrouped") {
    // the dq runner's own grouping codegen, no if_relevant involved
    groupedAgreesWithUngrouped(i => s"id >= 0 and id >= $i")(_ => ())
  }

  test("grouped if_relevant agrees with ungrouped, filter false is IgnoredRule") {
    // without filterFalseResults the grouped run answers Passed
    groupedAgreesWithUngrouped(i => s"if_relevant(id > 100000, id >= $i)")(
      _.distinct shouldBe Seq(IgnoredRule))
  }

  test("grouped if_relevant with a nullable filter agrees with ungrouped") {
    groupedAgreesWithUngrouped(i =>
      s"if_relevant(if(id % 2 = 0, cast(null as boolean), id > 100000), id >= $i)")(_ => ())
  }
}
