package com.sparkutils.qualityTests

import com.sparkutils.quality.impl.util.RuleSuiteDocs
import java.io.FileOutputStream
import RuleSuiteDocs.RelativeWarningsAndErrors
import com.sparkutils.quality.{ExpressionRule, Id, LambdaFunction, OutputExpression, Rule, RuleSet, RuleSuite, RunOnPassProcessor, validate}
import org.apache.commons.io.IOUtils
import org.junit.Test

class DocMarkdownTest extends TestUtils { // test utils to force spark session before register is called

  /**
   * doesn't test anything but it's helpfully bootstrapped
   */
  @Test
  def testMDRuleDocs = {
    com.sparkutils.quality.registerMapLookupsAndFunction(Map.empty)
    // Id(6,1) is on a rule, outputexpression and a lambda to force correct called by resolution
    val output2 = RunOnPassProcessor(0, Id(6,1), OutputExpression("testCaller2(fielda, fieldb)"))
    val rule1 = Rule(Id(2,1), ExpressionRule("/** description @param fielda desc */ concat(fielda, fieldb)"), output2)
    val output1 = RunOnPassProcessor(0, Id(1002,1), OutputExpression("/** description 2 @param fielda desc 2 */ concat(fielda, fieldb)"))
    val lambda1 = LambdaFunction("test", "/** lambda description @param fielda lambda desc */ variable -> variable", Id(6,1))
    val lambda2 = LambdaFunction("testCaller2", "(outervariable1, variable2) -> concat(outervariable1, variable2) and acos(fielda)", Id(7,2))
    val lambda3 = LambdaFunction("testCaller3", "/** lambda description only */ (outervariable1, variable2, variable3) -> testCaller2(outervariable1, variable2)", Id(8,1))

    val rule2 = Rule(Id(6,1), ExpressionRule("fielda > fieldb"), output1)
    val rule3 = Rule(Id(4,1), ExpressionRule("testCaller2(fielda > fieldb) and test(fieldb)"))
    val rule4 = Rule(Id(5,1), ExpressionRule("map_Lookup(fielda, fieldb) and test(fieldb)"))
    val rule5 = Rule(Id(16,1), ExpressionRule("nonExistentFunction(fielda) and nonExistentFielda > nonExistentFieldb"))
    val rs = RuleSuite(Id(0,1), Seq(RuleSet(Id(1,1), Seq(
      rule1,
      rule2,
      rule3,
      rule4,
      rule5
    ))), Seq(lambda1,
      lambda2,
      lambda3))
    val (errors, warnings, out, docs, expr) = validate(Left(new ValidationTest().struct), rs)

    val relative = RelativeWarningsAndErrors("../sampleDocsValidation/", errors, warnings)
    val md = RuleSuiteDocs.createMarkdown(docs, rs, expr, "../../sqlfunctions/", Some(relative))

    val docsf = new java.io.File(SparkTestUtils.docpath("sampleDocsOutput.md"))
    docsf.getParentFile.mkdirs
    IOUtils.write(md, new FileOutputStream(docsf))

    debug(println(md))

    val samplesf = new java.io.File(SparkTestUtils.docpath("sampleDocsValidation.md"))
    samplesf.getParentFile.mkdirs
    val emd = RuleSuiteDocs.createErrorAndWarningMarkdown(docs, rs, relative.copy( relativePath = "../sampleDocsOutput/"))
    IOUtils.write(emd, new FileOutputStream(samplesf))

    debug(println(emd))

  }
}
