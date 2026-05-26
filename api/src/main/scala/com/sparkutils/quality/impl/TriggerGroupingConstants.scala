package com.sparkutils.quality.impl

trait TriggerGroupingConstants {

  /**
   * Processing implementation for trigger grouping.  This is used by all engine runners to access their
   * extraConfig Map, then via getConfig and finally the runner appropriate default.
   */
  val groupProcessorKey = "quality.runnerGroupProcessor"

  /**
   * When present and true will trigger the dumpAudit process for a given group processing implementation.  By
   * default, no dumpAudit is run.
   */
  val groupProcessorAuditKey = "quality.runnerGroupProcessor.audit"

  /**
   * Defaulting to 130, the default bucket size can be overridden.  NOTE this size is a guide to bucketing only, use
   * dumpAudit via [[groupProcessorAuditKey]] to identify the optimum size for a given rule suite, this can take many
   * minutes to run.
   */
  val groupProcessorBucketSizeKey = "quality.runnerGroupProcessor.bucketSize"

  /**
   * Defaulting to "./", specifies the location of where to save any audit files
   */
  val groupProcessorAuditLocation = "quality.runnerGroupProcessor.auditLocation"

  /**
   * Defaulting to "classSimpleName", specifies the name for the audit file
   */
  val groupProcessorAuditName = "quality.runnerGroupProcessor.auditName"

  /**
   * Dumps compilation time for ruleRunners with SplitCompilation
   */
  val showSplitCompilationTime = "quality.showSplitCompilationTime"

  /**
   * Dumps grouping time
   */
  val showGroupingTime = "quality.showGroupingTime"
}
