package com.sparkutils.quality.impl

import com.sparkutils.quality.LambdaFunction
import com.sparkutils.quality.impl.extension.QualityFunctionParser.{CREATE_FUNCTION_PREFIX, DIVIDER, WITH_TOKEN}
import org.apache.spark.sql.qualityFunctions.LambdaFunctions
import org.apache.spark.sql.{SparkSession, classic}

/*
 When building 0.2.0 verify issues:

   java.lang.VerifyError: Bad type on operand stack
Exception Details:
  Location:
    org/apache/spark/sql/QualitySparkUtils$.$anonfun$execute$1(Lorg/apache/spark/sql/catalyst/plans/logical/LogicalPlan;Lorg/apache/spark/sql/catalyst/rules/Rule;)Lorg/apache/spark/sql/catalyst/plans/logical/LogicalPlan; @41: invokevirtual
  Reason:
    Type 'org/apache/spark/sql/catalyst/plans/logical/LogicalPlan' (current frame, stack[1]) is not assignable to 'org/apache/spark/sql/catalyst/trees/TreeNode'
  Current Frame:
    bci: @41
    flags: { }
    locals: { 'org/apache/spark/sql/catalyst/plans/logical/LogicalPlan', 'org/apache/spark/sql/catalyst/rules/Rule', top, 'scala/Tuple2', 'org/apache/spark/sql/catalyst/plans/logical/LogicalPlan', 'org/apache/spark/sql/catalyst/rules/Rule', long, long_2nd }
    stack: { 'org/apache/spark/sql/catalyst/rules/Rule', 'org/apache/spark/sql/catalyst/plans/logical/LogicalPlan' }
  Bytecode:
    0000000: bb00 8e59 2a2b b700 9a4e 2dc6 0029 2db6
    0000010: 0092 c000 f03a 042d b600 97c0 0393 3a05
    0000020: b803 9637 0619 0519 04b6 0399 c000 f03a
    0000030: 0819 08b0 a700 03bb 009c 592d b700 9fbf
    0000040:
  Stackmap Table:
    append_frame(@52,Top,Object[#142])
    same_frame(@55)
  at com.sparkutils.quality.impl.imports.LambdaFunctionsImports.registerLambdaFunctions(LambdaFunctionsImports.scala:19)

 were triggered on connect only clients as plan is no longer the same thing.  As such the split was introduced.
 */
object QualitySparkUtils {

  def registerLambdaFunctions(functions: Seq[LambdaFunction]): Unit =
    if (functions.nonEmpty)
      SparkSession.active match {
        case s: classic.SparkSession =>
          LambdaFunctions.registerLambdaFunctions(functions)
        case _ =>
          val s = SparkSession.active
          val command = s"$CREATE_FUNCTION_PREFIX\n" +
            functions.map {
              f =>
                // needs to be registered via the extension
                s"${f.name}$WITH_TOKEN${f.rule}"
            }.mkString(DIVIDER)
          s.sql(command)
      }
    else
      ()
}
