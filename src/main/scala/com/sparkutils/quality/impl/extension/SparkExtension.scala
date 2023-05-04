package com.sparkutils.quality.impl.extension

import com.sparkutils.quality.impl.extension.QualitySparkExtension.disableRulesConf
import com.sparkutils.quality.utils.Testing
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{QualitySparkUtils, SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.qualityFunctions.utils
import org.apache.spark.sql.types.DataType

object QualitySparkExtension {
  val disableRulesConf = "quality.disable.optimiser.rules"
}

/**
 * Registers Quality sql functions using the defaults for registerQualityFunctions, these can be overridden without having to subclass DriverPlugin.
 *
 * It also registers plan optimiser rule's such as AsUUIDFilter, which rewrites filters with variables backed by as_uuid.
 *
 * Optimiser rules can be disabled via the quality.disable.optimiser.rules system environment variable. "*" disables all rules, otherwise a comma separated list of fqn class names may be used.
 */
class QualitySparkExtension extends ((SparkSessionExtensions) => Unit) with Logging {

  def parseTypes: String => Option[DataType] = com.sparkutils.quality.defaultParseTypes _
  def zero: DataType => Option[Any] = com.sparkutils.quality.defaultZero _
  def add: DataType => Option[(Expression, Expression) => Expression] = (dataType: DataType) => com.sparkutils.quality.defaultAdd(dataType)
  def mapCompare: DataType => Option[(Any, Any) => Int] = (dataType: DataType) => utils.defaultMapCompare(dataType)
  def writer: String => Unit = println(_)

  /**
   * Adds AsymmetricFilterExpressions for AsUUID
   * Derived implementations should also call super.
   * These are registered after resolution is done.
   * The first String should be the fqn classname / rule name for the rule
   * @param sparkSession
   * @return
   */
  def optimiserRules: Seq[(String, SparkSession => Rule[LogicalPlan])] = Seq((AsUUIDFilter.getClass.getName, _ => AsUUIDFilter))

  override def apply(extensions: SparkSessionExtensions): Unit = {
    com.sparkutils.quality.registerQualityFunctions(parseTypes, zero, add, mapCompare, writer,
      register = QualitySparkUtils.registerFunctionViaExtension(extensions) _
    )

    if (Testing.testing) {
      ExtensionTesting.disableRuleResult = ""
    }
    val disableConf = com.sparkutils.quality.getConfig(disableRulesConf)
    if (disableConf != "*") {
      val disabledRules = disableConf.split(",").map(_.trim).toSet
      val filteredRules = optimiserRules.filterNot(p => disabledRules.contains(p._1.trim))
      val str = s"$disableRulesConf = $disabledRules leaving ${filteredRules.map(_._1)} remaining"
      logInfo(str)
      println(s"Quality SparkExtensions: $str")
      if (Testing.testing) {
        ExtensionTesting.disableRuleResult = str
      }
      filteredRules.map(_._2).foreach(extensions.injectOptimizerRule _)
    } else {
      val str = s"All optimiser rules are disabled via $disableRulesConf"
      logInfo(str)
      println(s"Quality SparkExtensions: $str")
    }
  }

}

private[sparkutils] object ExtensionTesting {
  // whilst clearly not threadsafe it should only be called during single threaded 'driver' testing
  var disableRuleResult: String = ""
}