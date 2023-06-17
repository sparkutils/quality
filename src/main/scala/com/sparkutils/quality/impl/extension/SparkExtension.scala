package com.sparkutils.quality.impl.extension

import com.sparkutils.quality.impl.extension.QualitySparkExtension.{disableRulesConf, forceInjectFunction}
import com.sparkutils.quality.utils.Testing
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{QualitySparkUtils, SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.expressions.{Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.qualityFunctions.utils
import org.apache.spark.sql.types.DataType

object QualitySparkExtension {
  /**
   * underscores as . isn't valid in an env name and only env / system property is available when apply is called
   */
  val disableRulesConf = "quality_disable_optimiser_rules"
  val forceInjectFunction = "quality_force_inject_function"
}

/**
 * Registers Quality sql functions using the defaults for registerQualityFunctions, these can be overridden without having to subclass DriverPlugin.  Functions are registered via FunctionRegistry.builtIn making them available for view creation, if they should be registered via extensions only then use the quality_force_inject_function = true configuration.
 *
 * It also registers plan optimiser rule's such as AsUUIDFilter, which rewrites filters with variables backed by as_uuid.
 *
 * Optimiser rules can be disabled via the quality_disable_optimiser_rules system environment variable. "*" disables all rules, otherwise a comma separated list of fqn class names may be used.
 */
class QualitySparkExtension extends ((SparkSessionExtensions) => Unit) with Logging {

  def parseTypes: String => Option[DataType] = com.sparkutils.quality.impl.RuleRegistrationFunctions.defaultParseTypes _
  def zero: DataType => Option[Any] = com.sparkutils.quality.impl.RuleRegistrationFunctions.defaultZero _
  def add: DataType => Option[(Expression, Expression) => Expression] = (dataType: DataType) => com.sparkutils.quality.impl.RuleRegistrationFunctions.defaultAdd(dataType)
  def mapCompare: DataType => Option[(Any, Any) => Int] = (dataType: DataType) => utils.defaultMapCompare(dataType)
  def writer: String => Unit = println(_)

  /**
   * uses writer to write prefix: str
   * @param str
   * @param prefix defaults to "Quality SparkExtensions"
   */
  def dump(str: String, prefix: String = "Quality SparkExtensions") = writer(s"$prefix: $str")

  /**
   * attempts to logInfo, typically doesn't work, but also then dumps the str
   * @param str
   */
  def attemptLogInfo(str: String) = {
    logInfo(str)
    dump(str)
  }

  /**
   * Adds AsymmetricFilterExpressions for AsUUID
   * Derived implementations should also call super if they wish to register the default rules (or specify both as extensions).
   * These are registered after resolution is done.
   * The first String should be the fqn classname / rule name for the rule
   * @return
   */
  def optimiserRules: Seq[(String, SparkSession => Rule[LogicalPlan])] =
    Seq((AsUUIDFilter.getClass.getName, _ => AsUUIDFilter), (IDBase64Filter.getClass.getName, _ => IDBase64Filter))

  override def apply(extensions: SparkSessionExtensions): Unit = {
    val func =
      if (com.sparkutils.quality.getConfig(forceInjectFunction, "false").toBoolean) {
        attemptLogInfo("registering quality functions via injection - they are classed as temporary functions")
        QualitySparkUtils.registerFunctionViaExtension(extensions) _
      } else {
        attemptLogInfo("registering quality functions via builtin function registry - whilst you can use these in global views the extension must always be present")
        QualitySparkUtils.registerFunctionViaBuiltin _
      }
    com.sparkutils.quality.registerQualityFunctions(parseTypes, zero, add, mapCompare, writer,
      registerFunction = func
    )

    if (Testing.testing) {
      ExtensionTesting.disableRuleResult = ""
    }
    val disableConf = com.sparkutils.quality.getConfig(disableRulesConf)
    if (disableConf != "*") {
      val disabledRules = disableConf.split(",").map(_.trim).toSet
      val filteredRules = optimiserRules.filterNot(p => disabledRules.contains(p._1.trim))
      val str = s"$disableRulesConf = $disabledRules leaving ${filteredRules.map(_._1)} remaining"
      attemptLogInfo(str)
      if (Testing.testing) {
        ExtensionTesting.disableRuleResult = str
      }
      filteredRules.map(_._2).foreach(extensions.injectOptimizerRule _)
    } else {
      val str = s"All optimiser rules are disabled via $disableRulesConf"
      attemptLogInfo(str)
    }
  }

}

private[sparkutils] object ExtensionTesting {
  // whilst clearly not threadsafe it should only be called during single threaded 'driver' testing
  var disableRuleResult: String = ""
}