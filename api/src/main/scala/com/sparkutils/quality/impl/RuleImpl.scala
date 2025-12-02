package com.sparkutils.quality.impl

import com.sparkutils.quality
import com.sparkutils.quality.RunOnPassProcessor.RunOnPassProcessorImpl
import com.sparkutils.quality.impl.imports.RuleResultsImports.{DisabledRuleInt, FailedInt, PassedInt}
import com.sparkutils.quality.{DisabledRule, Failed, Id, OutputExpression, Passed, Probability, RuleResult, RuleResultWithProcessor, RuleSuite, RunOnPassProcessor, SoftFailed, SoftFailedInt}
import com.sparkutils.quality.impl.util.Serializing.toSeq
import org.apache.spark.sql.SparkSession

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream, ObjectStreamClass}

object RuleSuiteHelpers {
  def getSparkClassLoader: ClassLoader = classOf[SparkSession].getClassLoader

  def getContextOrSparkClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)

  protected[quality] def deserialize(in: Array[Byte]): RuleSuite = {
    val os = new ObjectInputStream(new ByteArrayInputStream(in)) {
      override def resolveClass(desc: ObjectStreamClass): Class[_] =
        Class.forName(desc.getName, false, getContextOrSparkClassLoader)
    }
    val suite = os.readObject()
    os.close()
    suite.asInstanceOf[RuleSuite]
  }

  protected[quality] def serialize(ruleSuite: RuleSuite): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val os = new ObjectOutputStream(bos)
    // get rid of List's Vectors are serializable
    os.writeObject(toSeq(ruleSuite))
    val res = bos.toByteArray
    os.close()
    res
  }

  def ruleResultToInt(ruleResult: RuleResult): Int =
    ruleResult match {
      case Failed => FailedInt
      case SoftFailed => SoftFailedInt
      case DisabledRule => DisabledRuleInt
      case Passed => PassedInt
      case Probability(percentage) => (percentage * PassedInt).toInt
      case RuleResultWithProcessor(res, _) => ruleResultToInt(res)
    }

}

case class HolderUsedInsteadIfImpl(id: Id) extends
  RuntimeException(s"An OutputExpression $id has either not been correctly linked in your rules or you have not called withExpr.")

@SerialVersionUID(1L)
case class RunOnPassProcessorHolder(salience: Int, id: Id) extends RunOnPassProcessor with Serializable {

  lazy val rule: String = throw HolderUsedInsteadIfImpl(id)

  override def withExpr(expr: quality.OutputExpression): RunOnPassProcessor =
    RunOnPassProcessorImpl(salience, id, expr.rule)
}
