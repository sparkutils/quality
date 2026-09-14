package com.sparkutils.qualityTests

import com.sparkutils.quality._
import com.sparkutils.quality.implicits._

class GroupAuditTest extends GroupTestBase {

  val group_function: String = "group_audit"

  test("dq results should group with array") {
    dqResultsShouldGroup()
  }

  test("engine results should group with array") {
    type T = RuleSuiteGroupResults
    engineResultShouldGroup[T](identity)
  }

  test("folder results should group with array") {
    type T = RuleSuiteGroupResults
    folderResultShouldGroup[T](identity)
  }

  test("collector results should group with array") {
    type T = RuleSuiteGroupResults
    collectorResultShouldGroup[T](identity)
  }

  test("result groups should group with array") {
    resultGroupsShouldGroup()
  }

  test("collector results groups should group with array") {
    type T = RuleSuiteGroupResults

    //TODO test R value
    val r = collectorResultGroupsShouldGroup[T](identity)
  }

  test("dq results should group") {
    dqResultsShouldGroup(arrayStart = "", arrayEnd = "")
  }

  test("engine results should group") {
    type T = RuleSuiteGroupResults
    engineResultShouldGroup[T](_, arrayStart = "", arrayEnd = "")
  }

  test("folder results should group") {
    type T = RuleSuiteGroupResults
    folderResultShouldGroup[T](_, arrayStart = "", arrayEnd = "")
  }

  test("collector results should group") {
    type T = RuleSuiteGroupResults
    collectorResultShouldGroup[T](_, arrayStart = "", arrayEnd = "")
  }

  test("result groups should group") {
    resultGroupsShouldGroup(arrayStart = "", arrayEnd = "")
  }

  test("collector results groups should group") {
    type T = RuleSuiteGroupResults

    //TODO test R value
    val r = collectorResultGroupsShouldGroup[T](identity)
  }
}
