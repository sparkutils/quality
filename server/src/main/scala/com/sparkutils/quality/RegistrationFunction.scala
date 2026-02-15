package com.sparkutils.quality

object RegistrationFunction {
  /**
   * Simplified registerQualityFunctions, use classicFunction.registerQualityFunctions when the other features are needed.
   *
   */
  def registerQualityFunctions(): Unit = com.sparkutils.quality.classicFunctions.registerQualityFunctions()
}
