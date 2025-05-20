package com.sparkutils.quality.impl.util

import com.sparkutils.quality.{RuleSuite, ruleFolderRunner}
import org.apache.spark.sql.{Column, DataFrame, ShimUtils, Row => SRow}
import org.apache.spark.sql.QualitySparkUtils.DatasetBase
import org.apache.spark.sql.types.StructType

import javax.swing.text.html.HTML.Attribute

protected[quality] object AddDataFunctions {

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
  def ifoldAndReplaceFields[P[R] >: DatasetBase[R]](rules: RuleSuite, fields: Either[Seq[String], Seq[(String, Column)]], foldFieldName: String = "foldedFields",
                           debugMode: Boolean = false, tempFoldDebugName: String = "tempFOLDDEBUG",
                           maintainOrder: Boolean = true, useType: Option[StructType] = None,
                           compileEvals: Boolean = false, forceRunnerEval: Boolean = false,
                           forceTriggerEval: Boolean = false): P[SRow] => P[SRow] = rdf => {
    val df = rdf.asInstanceOf[DataFrame]
    import org.apache.spark.sql.functions._

    val fieldNames = fields.fold(identity, _.map(_._1))

    val theStruct = fields.fold( fields =>
      struct(fields.head, fields.tail :_*),
      pairs =>
        named_struct(pairs.flatMap(p => Seq(lit(p._1), p._2)) :_*)
    )
    val withFolder =
      df.select(expr("*"), ruleFolderRunner(rules, theStruct, debugMode = debugMode, useType = useType,
       compileEvals = compileEvals, forceRunnerEval = forceRunnerEval, forceTriggerEval = forceTriggerEval).as(foldFieldName))

    // create now as the schema will have the folder, which we may want to keep
    val namesInOrder =
      if (maintainOrder)
        withFolder.schema.map(_.name)
      else
        Seq()
    // lift the results
    val result =
      if (debugMode)
        withFolder.drop(fieldNames : _*).selectExpr("*",
          s"if(size($foldFieldName.result) == 0 or $foldFieldName.result is null, null, element_at($foldFieldName.result, -1)).result as $tempFoldDebugName"
        ).selectExpr("*", s"$tempFoldDebugName.*").drop(tempFoldDebugName)
      else
        withFolder.drop(fieldNames : _*).selectExpr("*", s"$foldFieldName.result.*" )

    // bring back to top level in the correct order
    if (maintainOrder)
      result.select(namesInOrder.head, namesInOrder.tail :_*)
    else
      result
  }.asInstanceOf[P[SRow]]

}