package com.sparkutils.qualityTests

import com.sparkutils.testing.TestRunner

object QualityTestRunner extends TestRunner {

  val packageName: String = "com.sparkutils"

  val projectName: String = "Quality"

  override val classLoader: ClassLoader = classOf[RemoteFunctionTests].getClassLoader
  def main(args: Array[String]): Unit = {
    test(args)
  }
}
