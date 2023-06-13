package com.sparkutils.quality.utils

import com.sparkutils.quality.RuleSuite
import com.sparkutils.quality.impl.{RuleEngineRunner, RuleEngineRunnerImpl, RuleRunner, RuleRunnerImpl}
import com.sparkutils.quality.utils.AddDataFunctions.ifoldAndReplaceFields
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Dataset, Row}

trait AddDataFunctionsImports {


  /**
   * Adds a DataQuality field using the RuleSuite and RuleSuiteResult structure
   * @param dataFrame
   * @param rules
   * @param name
   * @return
   */
  def addDataQuality(dataFrame: Dataset[Row], rules: RuleSuite, name: String = "DataQuality"): Dataset[Row] = {
    import org.apache.spark.sql.functions.expr
    dataFrame.select(expr("*"), RuleRunnerImpl.ruleRunnerImpl(rules).as(name))
  }

  /**
   * Adds a DataQuality field using the RuleSuite and RuleSuiteResult structure for use with dataset.transform functions
   * @param rules
   * @param name
   * @return
   */
  def addDataQualityF(rules: RuleSuite, name: String = "DataQuality"): Dataset[Row] => Dataset[Row] =
    addDataQuality(_, rules, name)

  /**
   * Adds two columns, one for overallResult and the other the details, allowing 30-50% performance gains for simple filters
   * @param dataFrame
   * @param rules
   * @param overallResult
   * @param resultDetails
   * @return
   */
  def addOverallResultsAndDetails(dataFrame: Dataset[Row], rules: RuleSuite, overallResult: String = "DQ_overallResult",
                                  resultDetails: String = "DQ_Details"): Dataset[Row] = {
    val temporaryDQname: String = "DQ_TEMP_Quality"
    addDataQuality(dataFrame, rules, temporaryDQname).
      selectExpr("*",s"$temporaryDQname.overallResult as $overallResult",
        s"ruleSuiteResultDetails($temporaryDQname) as $resultDetails").drop(temporaryDQname)
  }

  /**
   * Adds two columns, one for overallResult and the other the details, allowing 30-50% performance gains for simple filters, for use in dataset.transform functions
   * @param rules
   * @param overallResult
   * @param resultDetails
   * @return
   */
  def addOverallResultsAndDetailsF(rules: RuleSuite, overallResult: String = "DQ_overallResult",
                                   resultDetails: String = "DQ_Details"): Dataset[Row] => Dataset[Row] =
    addOverallResultsAndDetails(_, rules, overallResult, resultDetails)

  /**
   * Leverages the foldRunner to replace fields, the input fields are used to create a structure that the rules fold over.
   * The fields are then dropped from the original Dataframe and added back from the resulting structure.
   *
   * NOTE: The field order and types of the original DF will be maintained only when maintainOrder is true.  As it requires access to the schema it may incur extra work.
   *
   * @param rules
   * @param debugMode when true the last results are taken for the replaced fields
   * @param maintainOrder when true the schema is used to replace fields in the correct location, when false they are simply appended
   * @return
   */
  def foldAndReplaceFields(rules: RuleSuite, fields: Seq[String], foldFieldName: String = "foldedFields", debugMode: Boolean = false,
                           tempFoldDebugName: String = "tempFOLDDEBUG",
                           maintainOrder: Boolean = true): Dataset[Row] => Dataset[Row] =
    ifoldAndReplaceFields(rules, fields, foldFieldName,
      debugMode, tempFoldDebugName, maintainOrder)

  /**
   * Leverages the foldRunner to replace fields, the input fields are used to create a structure that the rules fold over.
   * The fields are then dropped from the original Dataframe and added back from the resulting structure.
   *
   * This version should only be used when you require select(*, foldRunner) to be used, it requires you fully specify types.
   *
   * NOTE: The field order and types of the original DF will be maintained only when maintainOrder is true.  As it requires access to the schema it may incur extra work.
   *
   * @param rules
   * @param struct The fields, and types, are used to call the foldRunner.  These types must match in the input fields
   * @param debugMode when true the last results are taken for the replaced fields
   * @param maintainOrder when true the schema is used to replace fields in the correct location, when false they are simply appended
   * @return
   */
  def foldAndReplaceFieldsWithStruct(rules: RuleSuite, struct: StructType, foldFieldName: String = "foldedFields", debugMode: Boolean = false,
                                     tempFoldDebugName: String = "tempFOLDDEBUG",
                                     maintainOrder: Boolean = true): Dataset[Row] => Dataset[Row] =
    AddDataFunctions.ifoldAndReplaceFields(rules, struct.fields.map(_.name), foldFieldName,
      debugMode, tempFoldDebugName, maintainOrder, useType = Some(struct))

  /**
   * Leverages the ruleRunner to produce an new output structure, the outputType defines the output structure generated.
   *
   * This version should only be used when you require select(*, ruleRunner) to be used, it requires you fully specify types.
   *
   * @param rules
   * @param dataFrame the input dataframe
   * @param outputType The fields, and types, are used to call the foldRunner.  These types must match in the input fields
   * @param ruleEngineFieldName The field name the results will be stored in, by default ruleEngine
   * @param alias sets the alias to use for dataFrame when using subqueries to resolve ambiguities, setting to an empty string (or null) will not assign an alias
   * @return
   */
  def ruleEngineWithStruct(dataFrame: Dataset[Row], rules: RuleSuite, outputType: DataType, ruleEngineFieldName: String = "ruleEngine", alias: String = "main", debugMode: Boolean = false): Dataset[Row] = {
    import org.apache.spark.sql.functions.expr
    (if ( (alias eq null) || alias.isEmpty )
      dataFrame
    else
      dataFrame.as(alias)).select(expr("*"), RuleEngineRunnerImpl.ruleEngineRunnerImpl(rules, outputType, debugMode = debugMode).as(ruleEngineFieldName))
  }

  /**
   * Leverages the ruleRunner to produce an new output structure, the outputType defines the output structure generated.
   *
   * This version should only be used when you require select(*, ruleRunner) to be used, it requires you fully specify types.
   *
   * @param rules
   * @param outputType The fields, and types, are used to call the foldRunner.  These types must match in the input fields
   * @return
   */
  def ruleEngineWithStructF(rules: RuleSuite, outputType: DataType, ruleEngineFieldName: String = "ruleEngine", alias: String = "main", debugMode: Boolean = false): Dataset[Row] => Dataset[Row] =
    ruleEngineWithStruct(_, rules, outputType, ruleEngineFieldName, alias, debugMode)
}
