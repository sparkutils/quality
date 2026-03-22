package com.sparkutils.quality.impl

/**
 * Safe to use with connect
 */
object ReWriteConstants {

  val INC_REWRITE_GENEXP_ERR_MSG: String = "inc('DDL', generic expression) is not supported in NO_REWRITE mode, use inc(generic expression) without NO_REWRITE mode enabled"

  val RULE_SUITE_GROUPS_MISSING_ERR_MSG: String = "rule_suite_from called but no matching RuleSuite was found"

}
