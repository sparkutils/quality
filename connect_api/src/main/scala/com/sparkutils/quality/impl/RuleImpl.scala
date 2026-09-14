package com.sparkutils.quality.impl

import com.sparkutils.quality
import com.sparkutils.quality.impl.imports.RuleResultsImports.{DisabledRuleInt, FailedInt, IgnoredRuleInt, PassedInt, SoftFailedInt, UnevaluatedRuleInt}
import com.sparkutils.quality.{DefaultProcessor, DefaultRule, DefaultRuleInt, DisabledRule, Failed, Id, IgnoredRule, OutputExpression, Passed, Probability, RuleResult, RuleResultWithProcessor, RuleSuite, RuleSuiteGroup, RunOnPassProcessor, SoftFailed, UnevaluatedRule}
import com.sparkutils.quality.impl.util.Serializing.toSeq
import org.apache.spark.sql.SparkSession

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream, ObjectStreamClass}

object RuleSuiteHelpers {
  def getSparkClassLoader: ClassLoader = classOf[SparkSession].getClassLoader

  def getContextOrSparkClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)

  protected[quality] def deserializeImpl[T](in: Array[Byte]): T = {
    val os = new ObjectInputStream(new ByteArrayInputStream(in)) {
      override def resolveClass(desc: ObjectStreamClass): Class[_] =
        Class.forName(desc.getName, false, getContextOrSparkClassLoader)
    }
    val suite = os.readObject()
    os.close()
    suite.asInstanceOf[T]
  }

  protected[quality] def deserialize(in: Array[Byte]): RuleSuite = deserializeImpl[RuleSuite](in)


  protected[quality] def serializeImpl[T](in: T)(f: T => T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val os = new ObjectOutputStream(bos)
    // get rid of List's Vectors are serializable
    os.writeObject(f(in))
    val res = bos.toByteArray
    os.close()
    res
  }

  // sparkutils to allow tests to call
  protected[sparkutils] def serialize(ruleSuite: RuleSuite): Array[Byte] = serializeImpl(ruleSuite)(toSeq)

  protected[quality] def deserializeGroup(in: Array[Byte]): RuleSuiteGroup = deserializeImpl[RuleSuiteGroup](in)

  protected[quality] def serializeGroup(ruleSuiteGroup: RuleSuiteGroup): Array[Byte] =
    serializeImpl(ruleSuiteGroup)(r => r.copy(ruleSuites = r.ruleSuites.map(p => p._1 -> toSeq(p._2))))

  def ruleResultToInt(ruleResult: RuleResult): Int =
    ruleResult match {
      case Failed => FailedInt
      case SoftFailed => SoftFailedInt
      case DisabledRule => DisabledRuleInt
      case IgnoredRule => IgnoredRuleInt
      case DefaultRule => DefaultRuleInt
      case Passed => PassedInt
      case Probability(percentage) => (percentage * PassedInt).toInt
      case RuleResultWithProcessor(res, _) => ruleResultToInt(res)
      case UnevaluatedRule => UnevaluatedRuleInt
    }

}

case class HolderUsedInsteadIfImpl(id: Id) extends
  RuntimeException(s"An OutputExpression $id has either not been correctly linked in your rules or you have not called withExpr.")

@SerialVersionUID(1L)
case class RunOnPassProcessorHolder(salience: Int, id: Id) extends quality.RunOnPassProcessor with Serializable {

  lazy val rule: String = throw HolderUsedInsteadIfImpl(id)
  lazy val returnIfPassed: OutputExpression = throw HolderUsedInsteadIfImpl(id)

  override def withExpr(expr: quality.OutputExpression): quality.RunOnPassProcessor =
    RunOnPassProcessor.RunOnPassProcessorImpl(salience, id, expr match {
      case h: quality.HasRuleText => h.rule
      case _ => ""
    }, expr)
}

@SerialVersionUID(1L)
case class DefaultProcessorHolder(id: Id) extends quality.DefaultProcessor with Serializable {

  lazy val rule: String = throw HolderUsedInsteadIfImpl(id)
  lazy val outputExpression: OutputExpression = throw HolderUsedInsteadIfImpl(id)

  override def withExpr(expr: quality.OutputExpression): quality.DefaultProcessor =
    DefaultProcessor.DefaultProcessorImpl(id, expr match {
      case h: quality.HasRuleText => h.rule
      case _ => ""
    }, expr)
}
