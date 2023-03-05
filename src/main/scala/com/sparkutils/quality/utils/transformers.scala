package com.sparkutils.quality.utils

import com.sparkutils.quality.{RuleSuite, ruleFolderRunner, ruleRunner}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, Dataset, Row}

trait AddDataFunctions {

  /**
   * Adds a DataQuality field using the RuleSuite and RuleSuiteResult structure
   * @param dataFrame
   * @param rules
   * @param name
   * @return
   */
  def addDataQuality(dataFrame: Dataset[Row], rules: RuleSuite, name: String = "DataQuality"): Dataset[Row] = {
    import org.apache.spark.sql.functions.expr
    dataFrame.select(expr("*"), ruleRunner(rules).as(name))
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
   * @param useType In the case you must use select and can't use withColumn you may provide a type directly to stop the NPE
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
   * @param useType In the case you must use select and can't use withColumn you may provide a type directly to stop the NPE
   * @return
   */
  def foldAndReplaceFieldsWithStruct(rules: RuleSuite, struct: StructType, foldFieldName: String = "foldedFields", debugMode: Boolean = false,
                           tempFoldDebugName: String = "tempFOLDDEBUG",
                           maintainOrder: Boolean = true): Dataset[Row] => Dataset[Row] =
    ifoldAndReplaceFields(rules, struct.fields.map(_.name), foldFieldName,
      debugMode, tempFoldDebugName, maintainOrder, useType = Some(struct))

  /**
   * Leverages the foldRunner to replace fields, the input fields are used to create a structure that the rules fold over.
   * The fields are then dropped from the original Dataframe and added back from the resulting structure.
   *
   * NOTE: The field order and types of the original DF will be maintained only when maintainOrder is true.  As it requires access to the schema it may incur extra work.
   *
   * @param rules
   * @param debugMode when true the last results are taken for the replaced fields
   * @param maintainOrder when true the schema is used to replace fields in the correct location, when false they are simply appended
   * @param useType In the case you must use select and can't use withColumn you may provide a type directly to stop the NPE
   * @return
   */
  protected def ifoldAndReplaceFields(rules: RuleSuite, fields: Seq[String], foldFieldName: String = "foldedFields", debugMode: Boolean = false,
                           tempFoldDebugName: String = "tempFOLDDEBUG",
                           maintainOrder: Boolean = true, useType: Option[StructType] = None): Dataset[Row] => Dataset[Row] = df => {
    import org.apache.spark.sql.functions._

    val theStruct = struct(fields.head, fields.tail :_*)
    val withFolder = {
      // select NPEs needs projection to work
      //df.select(expr("*"), ruleFolderRunner(rules, theStruct).as(foldFieldName))
      df.withColumn(foldFieldName, ruleFolderRunner(rules, theStruct, debugMode = debugMode, useType = useType) )
    }

    // create now as the schema will have the folder, which we may want to keep
    val namesInOrder =
      if (maintainOrder)
        withFolder.schema.map(_.name)
      else
        Seq()
    // lift the results
    val result =
      if (debugMode)
        withFolder.drop(fields : _*).selectExpr("*",
          s"element_at($foldFieldName.result, -1).result as $tempFoldDebugName"
        ).selectExpr("*", s"$tempFoldDebugName.*").drop(tempFoldDebugName)
      else
        withFolder.drop(fields : _*).selectExpr("*", s"$foldFieldName.result.*" )

    // bring back to top level in the correct order
    if (maintainOrder)
      result.select(namesInOrder.head, namesInOrder.tail :_*)
    else
      result
  }
}