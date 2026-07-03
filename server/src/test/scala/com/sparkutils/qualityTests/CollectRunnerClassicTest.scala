package com.sparkutils.qualityTests

import com.sparkutils.quality.{groupProcessorKey, topLevelBooleanGrouper}
import com.sparkutils.qualityTests.util.SharedConnectTests

class CollectRunnerClassicTest extends SharedConnectTests with CollectRunnerTestBase {

  override def thunker(thunk: => Unit): Unit =  evalCodeGensNoResolve {
    funNRewrites {
      thunk
    }
  }

}

class CollectRunnerClassicGrouperTest extends CollectRunnerClassicTest with CollectRunnerTestBase {

  override def options: Map[String, String] = Map(
    groupProcessorKey -> topLevelBooleanGrouper
  )

}
