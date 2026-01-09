package com.sparkutils.qualityTests

import com.sparkutils.testing.TestRunner
import com.sparkutils.testing.TestUtilsEnvironment.setupDefaultsViaCurrentSession

object QualityTestRunner extends TestRunner {

  val packageName: String = "com.sparkutils"

  val projectName: String = "Quality"

  override val classLoader: ClassLoader = classOf[RemoteFunctionTests].getClassLoader

  // when on fabric or databricks disables cluster tests
  setupDefaultsViaCurrentSession()

  def main(args: Array[String]): Unit = test(args)
}
