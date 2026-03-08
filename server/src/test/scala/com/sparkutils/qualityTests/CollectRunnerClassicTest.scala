package com.sparkutils.qualityTests

import com.sparkutils.qualityTests.util.SharedConnectTests

class CollectRunnerClassicTest extends SharedConnectTests with CollectRunnerTestBase {

  override def thunker(thunk: => Unit): Unit =  evalCodeGensNoResolve {
    funNRewrites {
      thunk
    }
  }
}
