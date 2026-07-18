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

class CollectRunnerClassicGrouperTest extends CollectRunnerClassicTest {

  override def options: Map[String, String] = {
    var m = Map.empty[String, String]
    not3_0_or_3_1 {

      // just duplicates the test run on 3 and 3.1
      m =
        Map(
          groupProcessorKey -> topLevelBooleanGrouper
        )
    }
    m
  }

}
