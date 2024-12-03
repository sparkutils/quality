package com.sparkutils.qualityTests

import com.sparkutils.quality.impl.util.{Docs, RuleSuiteDocs, WithDocs}
import com.sparkutils.quality.impl.{DataFrameSyntaxError, ExtraDocParameter, HasId, LambdaMultipleImplementationWithSameArityError, LambdaNameError, LambdaPossibleSOE, LambdaSparkFunctionNameError, LambdaStackOverflowError, LambdaSyntaxError, LambdaViewError, NonLambdaDocParameters, OuputSparkFunctionNameError, OutputRuleNameError, OutputRuleSyntaxError, OutputRuleViewError, RuleError, RuleNameError, RuleSyntaxError, RuleViewError, RuleWarning, SparkFunctionNameError, Validation}
import com.sparkutils.quality.impl.util.RuleSuiteDocs.{LambdaId, OutputExpressionId, RuleId}
import com.sparkutils.quality.{ExpressionRule, Id, LambdaFunction, OutputExpression, Rule, RuleSet, RuleSuite, RunOnPassProcessor, impl, ruleEngineRunner, ruleRunner, validate}
import Validation.emptyDocs
import impl.imports.RuleResultsImports.packId
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.junit.Test
import org.scalatest.FunSuite

class ValidationTest extends FunSuite with TestUtils {

  val struct = StructType(Seq(
    StructField("fielda", StringType),
    StructField("fieldb", IntegerType),
    StructField("fieldc", LongType),
    StructField("fieldd", DoubleType),
    StructField("fielde", BooleanType),
    StructField("nesty", StructType(
      Seq(
        StructField("morenesty", StructType(
          Seq(
            StructField("nesteda", IntegerType)
          )
        )),
        StructField("nestedb", IntegerType)
      ))
    )
  ))

  def ordered[T <: HasId](withIds: Set[T]) =
    withIds.toSeq.sortBy(error => (error.id.id, error.id.version))

  // drops warnings
  def ordered(errors: (Set[RuleError], Set[RuleWarning])) =
    ordered[RuleError](errors._1)

  val ossLT330SingleParam = "mismatched input '->'"
  val ossGT320SingleParam = "Syntax error at or near '->"

  @Test
  def testLambdaSyntaxError: Unit = {
    val rs = RuleSuite(Id(0,1), Seq(), Seq(LambdaFunction("test", "(variable) -> thelambda", Id(1,1))))
    val errors = ordered( validate(struct, rs) )

    assert(errors match {
      // for < 3.2.0 oss and 10.4 lts Databricks
      case Seq(LambdaSyntaxError(Id(1,1), err)) if err.contains(ossLT330SingleParam) => true
      // for 3.3.0 oss
      case Seq(LambdaSyntaxError(Id(1,1), err)) if err.contains(ossGT320SingleParam) => true
      case _ => false
    } )
  }

  @Test
  def testLambdaNameError: Unit = {
    val rs = RuleSuite(Id(0,1), Seq(), Seq(LambdaFunction("test", "variable -> thelambda", Id(1,1))))
    val errors = ordered( validate(struct, rs) )

    assert(errors match {
      case Seq(LambdaNameError("thelambda", Id(1,1))) => true
      case _ => false
    } )
  }

  @Test
  def testLambdaStackError: Unit = {
    val rs = RuleSuite(Id(0,1), Seq(), Seq(LambdaFunction("test", "variable -> test(outervariable)", Id(1,1))))
    val (errors, warns, shown, docs, expr) = validate(Left(struct), rs)

    assert(errors.toSeq match {
      case Seq(LambdaNameError("outervariable", Id(1,1))) => true
      case _ => false
    } )
    assert(warns.toSeq match {
      case Seq(LambdaPossibleSOE(Id(1,1))) => true
      case _ => false
    } )

  }

  @Test
  def testLambdaActualStackError: Unit = {
    val rs = RuleSuite(Id(0,1), Seq(), Seq(
      LambdaFunction("thetest", "variable -> test(variable - 112)", Id(3,1)),
      LambdaFunction("retest", "variable -> thetest(variable - 112)", Id(2,1)),
      LambdaFunction("test", "variable -> retest(variable + 1)", Id(1,1))
      ))
    val (errors, warns, shown, docs, expr) = validate(Left(struct), rs)

    warns.size // to allow debug of warns

    assert(errors.toSeq match {
      case Seq(LambdaStackOverflowError(Validation.unknownSOEId)) => true
      case _ => false
    } )

  }

  @Test
  def testLambdaActualStackErrorSuppressed: Unit = {
    val rs = RuleSuite(Id(0,1), Seq(), Seq(
      LambdaFunction("thetest", "variable -> test(variable - 112)", Id(3,1)),
      LambdaFunction("retest", "variable -> thetest(variable - 112)", Id(2,1)),
      LambdaFunction("test", "variable -> retest(variable + 1) + unknownVariable", Id(1,1))
    ))
    val (errors, warns, shown, docs, exprLookup) = validate(Left(struct), rs, runnerFunction = Some(df => expr("test(fielda)")),
      recursiveLambdasSOEIsOk = true)

    warns.size // to allow debug of warns

    assert(errors.toSeq match {
      // should be upset that test is unknown and we shouldn't get an SOE,
      // but because of the SOE we won't get unknownVariable name error either
      // Spark 4 uses routine instead of function
      case Seq(DataFrameSyntaxError(err)) if err.contains("test") && (err.contains("function") || err.contains("routine"))  => true
      case _ => false
    } )

  }

  @Test
  def testLambdaSparkUnknownNameError: Unit = {
    val rs = RuleSuite(Id(0,1), Seq(), Seq(
      LambdaFunction("testCaller", "outervariable -> funky(outervariable)", Id(2,2))
    ))
    val errors = ordered( validate(struct, rs) )

    assert(errors match {
      case Seq(LambdaSparkFunctionNameError("funky", Id(2,2))) => true
      case _ => false
    } )
  }

  @Test
  def testNestedFieldDoesntError: Unit = {
    val rs = RuleSuite(Id(0,1), Seq(), Seq(
      LambdaFunction("testCaller", "nesty.morenesty.nesteda > nesty.nestedb", Id(2,2))
    ))
    val errors = ordered( validate(struct, rs) )

    assert(errors.isEmpty)
  }

  @Test
  def testNestedFieldLambdaError: Unit = {
    val rs = RuleSuite(Id(0,1), Seq(), Seq(
      LambdaFunction("testCaller", "nesty.morenesty.nestedb > nesty.nestedb", Id(2,2))
    ))
    val errors = ordered( validate(struct, rs) )

    assert(errors match {
      case Seq(LambdaNameError("nesty.morenesty.nestedb", Id(2,2))) => true
      case _ => false
    } )
  }

  @Test
  def testLambdaNestedNameError: Unit = {
    val rs = RuleSuite(Id(0,1), Seq(), Seq(LambdaFunction("test", "variable -> outervariable", Id(1,1)),
      LambdaFunction("testCaller", "outervariable -> test(outervariable)", Id(1,2))
    ))
    val errors = ordered( validate(struct, rs) )

    assert(errors match {
      case Seq(LambdaNameError("outervariable", Id(1,1))) => true
      case _ => false
    } )
  }

  @Test
  def testLambdaNestedOverloadedNameError: Unit = {
    val rs = RuleSuite(Id(0,1), Seq(), Seq(LambdaFunction("test", "variable -> outervariable", Id(1,1)),
      LambdaFunction("test", "(outervariable, param) -> test(outervariable)", Id(1,2))
    ))
    val errors = ordered( validate(struct, rs) )

    assert(errors match {
      case Seq(LambdaNameError("outervariable", Id(1,1))) => true
      case _ => false
    } )
  }

  @Test
  def testLambdaWithDuplicateArityError: Unit = {
    val rs = RuleSuite(Id(0,1), Seq(), Seq(LambdaFunction("testfun", "(variable, a, b) -> variable", Id(1,1)),
      LambdaFunction("testfun", "(outervariable, param, c) -> test(outervariable)", Id(2,1)),
      LambdaFunction("test", "outervariable -> outervariable", Id(3,2))
    ))
    val errors = ordered( validate(struct, rs) )

    assert(errors match {
      case Seq(LambdaMultipleImplementationWithSameArityError("testfun", 2, 3, theSet))
        if theSet.toSeq.sortBy(t => t.id -> t.version) == Seq(Id(1,1), Id(2,1)) => true
      case _ => false
    } )
  }

  @Test
  def testLambdaNestedOverloadedNameWithInterimError: Unit = {
    val rs = RuleSuite(Id(0,1), Seq(), Seq(LambdaFunction("test", "variable -> funtest(outervariable)", Id(1,1)),
      LambdaFunction("funtest", "variable -> outervariable", Id(3,1)),
      LambdaFunction("test", "(outervariable, param) -> test(outervariable)", Id(1,2))
    ))
    val errors = ordered( validate(struct, rs) )

    assert(errors match {
      case Seq(LambdaNameError("outervariable", Id(1,1)),LambdaNameError("outervariable", Id(3,1))) => true
      case _ => false
    } )
  }

  @Test
  def testFunctionSyntaxError: Unit = { // Spark 4 adds >> in SPARK-48168
    val rs = RuleSuite(Id(0,1), Seq(RuleSet(Id(1,1), Seq(Rule(Id(2,1), ExpressionRule("fielda >>>> fieldb"))))))
    val errors = ordered( validate(struct, rs) )

    assert(errors match {
      case Seq(RuleSyntaxError(Id(2,1), err)) if err.contains("input '>'") || err.contains("near '>'")
        => true
      case _ => false
    } )
  }

  @Test
  def testNestedFieldError: Unit = {
    val rs = RuleSuite(Id(0,1), Seq(RuleSet(Id(1,1), Seq(Rule(Id(2,1), ExpressionRule("nesty.morenesty.nestedb > nesty.nestedb"))))))
    val errors = ordered( validate(struct, rs) )

    assert(errors match {
      case Seq(RuleNameError("nesty.morenesty.nestedb", Id(2,1))) => true
      case _ => false
    } )
  }

  @Test
  def testFunctionNameError: Unit = {
    val rs = RuleSuite(Id(0,1), Seq(RuleSet(Id(1,1), Seq(Rule(Id(2,1), ExpressionRule("fielda > b"))))))
    val errors = ordered( validate(struct, rs) )

    assert(errors match {
      case Seq(RuleNameError("b", Id(2,1))) => true
      case _ => false
    } )
  }

  @Test
  def testOutputFunctionSyntaxError: Unit = {  // Spark 4 adds >> in SPARK-48168
    val rs = RuleSuite(Id(0,1), Seq(RuleSet(Id(1,1), Seq(Rule(Id(2,1), ExpressionRule("fielda > fieldb"),
      RunOnPassProcessor(0, Id(1001,1), OutputExpression("fielda >>>> fieldb"))
    )))))
    val errors = ordered( validate(struct, rs) )

    assert(errors match {
      case Seq(OutputRuleSyntaxError(Id(1001,1), err)) if err.contains("input '>'") || err.contains("near '>'")
        => true
      case _ => false
    } )
  }

  @Test
  def testOutputFunctionNameError: Unit = {
    val rs = RuleSuite(Id(0,1), Seq(RuleSet(Id(1,1), Seq(Rule(Id(2,1), ExpressionRule("fielda > fieldb"),
      RunOnPassProcessor(0, Id(1001,1), OutputExpression("fielda > b"))
    )))))
    val errors = ordered( validate(struct, rs) )

    assert(errors match {
      case Seq(OutputRuleNameError("b", Id(1001,1))) => true
      case _ => false
    } )
  }

  @Test
  def testAllTheThingsExceptLambdaSOE: Unit = { // Spark 4 adds >> in SPARK-48168
    val rs = RuleSuite(Id(0,1), Seq(RuleSet(Id(1,1), Seq(
      Rule(Id(2,1), ExpressionRule("fielda > fieldb"), RunOnPassProcessor(0, Id(1001,1), OutputExpression("fielda > b"))),
      Rule(Id(3,1), ExpressionRule("fielda > fieldb"), RunOnPassProcessor(0, Id(1002,1), OutputExpression("fielda >>>> fieldb"))),
      Rule(Id(4,1), ExpressionRule("fielda > b")),
      Rule(Id(5,1), ExpressionRule("fielda >>>> fieldb"))
    ))), Seq(LambdaFunction("test", "variable -> outervariable", Id(6,1)),
      LambdaFunction("testCaller", "outervariable -> test(outervariable)", Id(7,2)),
      LambdaFunction("testCaller2", "(outervariable) -> test(outervariable)", Id(8,1))))
    val errors = ordered( validate(struct, rs) )

    assert(errors match {
      case Seq(
        RuleNameError("b", Id(4,1)),
        RuleSyntaxError(Id(5,1), rerr),
        LambdaNameError("outervariable", Id(6,1)),
        LambdaSyntaxError(Id(8,1), lerr),
        OutputRuleNameError("b", Id(1001,1)),
        OutputRuleSyntaxError(Id(1002,1), oerr)
      ) if ((
          lerr.contains(ossLT330SingleParam) ||
           lerr.contains(ossGT320SingleParam)
        ) && oerr.contains("'>'") && rerr.contains("'>'"))
        => true
      case _ => false
    } )
  }

  @Test
  def testUnknownFunctionRuleRunnerError: Unit = {
    val rs = RuleSuite(Id(0, 1), Seq(RuleSet(Id(1, 1), Seq(Rule(Id(2, 1), ExpressionRule("fakeConcat(fielda, fieldb)"))))))
    doTestUnknownFunction(rs, df => ruleRunner(rs))
  }

  @Test
  def testTransformOutput: Unit = {
    val rs = RuleSuite(Id(0, 1), Seq(RuleSet(Id(1, 1), Seq(Rule(Id(2, 1), ExpressionRule("a > b"),
      runOnPassProcessor = RunOnPassProcessor(0, Id(1001,1), OutputExpression("concat(a, b)"))
    )))))

    val df = sparkSession.range(1).selectExpr("(id + 2) as a", "(id + 1) as b")

    val (errs, warns, shown, docs, expr) = validate(df, rs, runnerFunction = df => ruleEngineRunner(rs, resultDataType = StringType, debugMode = true),
      transformBeforeShow = df => {df.selectExpr("Quality.salientRule")})
    // for debug
    errs.size
    warns.size
    debug(println(shown))
    assert(shown.toLowerCase.contains("null"))
  }

  def doTestUnknownFunction(rs: RuleSuite, func: DataFrame => Column): Unit = {
    val empty = sparkSession.sparkContext.emptyRDD[Row]
    val df = sparkSession.createDataFrame(empty, struct)

    val (errs, warns, shown, docs, expr) = validate(df, rs, runnerFunction = func)
    val errors = ordered( (errs, warns) )

    assert(errors match {
      case Seq(DataFrameSyntaxError(err),
        SparkFunctionNameError("fakeConcat", Id(2,1)) | OuputSparkFunctionNameError("fakeConcat", Id(1001,1))
      ) if err.contains("fakeConcat") => true
      case _ => false
    } )
  }

  @Test
  def testUnknownFunctionRuleEngineRunnerError: Unit = {
    val rs = RuleSuite(Id(0, 1), Seq(RuleSet(Id(1, 1), Seq(Rule(Id(2, 1), ExpressionRule("fielda > fieldb"),
      runOnPassProcessor = RunOnPassProcessor(0, Id(1001,1), OutputExpression("fakeConcat(fielda, fieldb)"))
    )))))
    doTestUnknownFunction(rs, df => ruleEngineRunner(rs, resultDataType = StringType))
  }

  @Test
  def testShowFunction: Unit = {
    val rs = RuleSuite(Id(0, 1), Seq(RuleSet(Id(1, 1), Seq(Rule(Id(2, 1), ExpressionRule("id > fieldc")
    )))))

    val df = sparkSession.range(15).selectExpr("(id + 1) as fieldc", "id")

    val (errs, warns, shown, docs, expr) = validate(Right(df), rs, runnerFunction = Some((df: DataFrame) => ruleRunner(rs)))
    val errors = ordered( (errs, warns) )

    assert(errors.isEmpty)
    debug(println(shown))
    assert(shown.contains(packId(Id(1,1)).toString)) // 4294967297
  }


  @Test
  def testAllTheDocsWarnings: Unit = {
    val output2 = RunOnPassProcessor(0, Id(1001,1), OutputExpression("fielda > fieldb"))
    val rule1 = Rule(Id(2,1), ExpressionRule("/** description @param fielda desc */ fielda > fieldb"), output2)
    val output1 = RunOnPassProcessor(0, Id(1002,1), OutputExpression("/** description 2 @param fielda desc 2 */ fielda > fieldb"))
    val lambda1 = LambdaFunction("test", "/** lambda description @param fielda lambda desc */ variable -> variable", Id(6,1))
    val lambda2 = LambdaFunction("testCaller2", "(outervariable1, variable2) -> outervariable1", Id(7,2))
    val lambda3 = LambdaFunction("testCaller3", "/** lambda description only */ (outervariable1, variable2, variable3) -> outervariable1", Id(8,1))

    val rule2 = Rule(Id(3,1), ExpressionRule("fielda > fieldb"), output1)
    val rule3 = Rule(Id(4,1), ExpressionRule("fielda > fieldb"))
    val rule4 = Rule(Id(5,1), ExpressionRule("fielda > fieldb"))
    val rs = RuleSuite(Id(0,1), Seq(RuleSet(Id(1,1), Seq(
      rule1,
      rule2,
      rule3,
      rule4
    ))), Seq(lambda1,
      lambda2,
      lambda3))
    val (errors, warnings, out, docs, expr) = validate(Left(struct), rs)

    val sorted = ordered(warnings)
    assert(errors.isEmpty)
    assert(sorted match {
      case Seq(
      NonLambdaDocParameters(Id(2,1)),
      ExtraDocParameter(Id(6,1), "fielda"),
      NonLambdaDocParameters(Id(1002,1))
      )
      => true
      case _ => false
    } )
    assert(docs == RuleSuiteDocs(
      rules = Map(Id(2,1) -> WithDocs(rule1, Docs("description", Map("fielda" -> "desc"))),
        rule2.id -> WithDocs(rule2, emptyDocs),
        rule3.id -> WithDocs(rule3, emptyDocs),
        rule4.id -> WithDocs(rule4, emptyDocs)
      ),
      outputExpressions = Map(Id(1002,1) -> WithDocs(output1, Docs("description 2", Map("fielda" -> "desc 2"))),
        output2.id -> WithDocs(output2, emptyDocs)
      ),
      lambdas = Map(Id(6,1) -> WithDocs(lambda1, Docs("lambda description", Map("fielda" -> "lambda desc"))),
        lambda2.id -> WithDocs(lambda2, emptyDocs),
        Id(8,1) -> WithDocs(lambda3,Docs("lambda description only"))
    )))
  }

  @Test
  def testExpressionLookups: Unit = {
    com.sparkutils.quality.registerMapLookupsAndFunction(Map.empty)
    // test output, lambda and rule for lookups
    // Id(6,1) is on a rule, outputexpression and a lambda to force correct called by resolution
    val output2 = RunOnPassProcessor(0, Id(6,1), OutputExpression("testCaller2(fielda, fieldb)"))
    val rule1 = Rule(Id(2,1), ExpressionRule("/** description @param fielda desc */ concat(fielda, fieldb)"), output2)
    val output1 = RunOnPassProcessor(0, Id(1002,1), OutputExpression("/** description 2 @param fielda desc 2 */ concat(fielda, fieldb)"))
    val lambda1 = LambdaFunction("test", "/** lambda description @param fielda lambda desc */ variable -> variable", Id(6,1))
    val lambda2 = LambdaFunction("testCaller2", "(outervariable1, variable2) -> concat(outervariable1, variable2)", Id(7,2))
    val lambda3 = LambdaFunction("testCaller3", "/** lambda description only */ (outervariable1, variable2, variable3) -> concat(outervariable1, variable2)", Id(8,1))

    val rule2 = Rule(Id(6,1), ExpressionRule("fielda > fieldb"), output1)
    val rule3 = Rule(Id(4,1), ExpressionRule("testCaller2(fielda > fieldb)"))
    val rule4 = Rule(Id(5,1), ExpressionRule("mapLookup(fielda, fieldb) and test(fieldb)"))
    val rs = RuleSuite(Id(0,1), Seq(RuleSet(Id(1,1), Seq(
      rule1,
      rule2,
      rule3,
      rule4
    ))), Seq(lambda1,
      lambda2,
      lambda3))
    val (_, _, _, _, expr) = validate(Left(struct), rs)

    assert(expr(RuleId(rule3.id)).lambdas.contains(lambda2.id))
    assert(expr(RuleId(rule4.id)).lambdas.contains(lambda1.id))
    // it's registered so it's spark - should do a quality doc lookup at some stage
    assert(expr(RuleId(rule4.id)).sparkFunctions.contains("mapLookup"))

    assert(expr(LambdaId(lambda2.id)).sparkFunctions.contains("concat"))

    assert(expr(OutputExpressionId(output2.id)).lambdas.contains(lambda2.id))
    assert(expr(OutputExpressionId(output1.id)).sparkFunctions.contains("concat"))

  }

  @Test
  def testMissingViews: Unit = { // v3_4_and_above not necessary as it's not getting to analysis
    val output1 = RunOnPassProcessor(0, Id(6,1), OutputExpression("test(fielda)"))
    val rule1 = Rule(Id(2,1), ExpressionRule("concat(fielda, fieldb)"), output1)
    val lambda1 = LambdaFunction("test", "variable -> select max(i) from theview where i.variable = variable", Id(16,1))
    val output2 = RunOnPassProcessor(0, Id(7,1), OutputExpression("select max(i) from theview where i > fieldb"))
    val rule2 = Rule(Id(3,1), ExpressionRule("concat(fielda, fieldb)"), output2)
    val output3 = RunOnPassProcessor(0, Id(8,1), OutputExpression("test(fielda)"))
    val rule3 = Rule(Id(4,1), ExpressionRule("exists(select 0 from theview where i > fieldb)"), output3)

    val rs = RuleSuite(Id(0,1), Seq(RuleSet(Id(1,1), Seq(
      rule1, rule2, rule3
    ))), Seq(lambda1))
    val (err, warn, _, _, _) = validate(Left(struct), rs)

    assert(warn.isEmpty)
    val sorted = ordered(err)

    assert(sorted match {
      case Seq(
      RuleViewError("theview", Id(4,1)),
      OutputRuleViewError("theview", Id(7,1)),
      LambdaViewError("theview", Id(16,1))
      )
      => true
      case _ => false
    } )

    val (err2, warn2, _, _, _) = validate(Left(struct), rs, viewLookup = {
      case "theview" => true
      case _ => false
    })

    assert(warn2.isEmpty)
    assert(err2.isEmpty)
  }
}
