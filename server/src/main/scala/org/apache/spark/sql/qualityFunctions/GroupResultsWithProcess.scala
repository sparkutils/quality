package org.apache.spark.sql.qualityFunctions

import com.sparkutils.quality.impl.GroupResults.typeCheckText
import com.sparkutils.quality.impl.GroupResultsBase
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{Expression, HigherOrderFunction, NamedLambdaVariable}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataType

case class GroupResultsWithProcess(arguments: Seq[Expression], function: Expression) extends GroupResultsBase
  with Binder {

  lazy val ref = arguments.last.asInstanceOf[RefExpression]

  override def processResultType: DataType = function.dataType
  override def processResultNullable: Boolean = function.nullable

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(newChildren.dropRight(1), newChildren.last)

  override def processResult(row: InternalRow, a: ArrayData): Any = {
    // set up the variable to be evaluated
    elementVars.head match {
        case nlv: NamedLambdaVariable => nlv.value.set(a)
        case nlvg: NamedLambdaVariableCodeGen => nlvg.value = a
      }

    function.eval(row)
  }

  override def withFunction(function: Expression): HigherOrderFunction with Binder = copy(function = function)

  override def argsToBind: Seq[Expression] = Seq(arguments.last)

  override def checkInputDataTypes(): TypeCheckResult = {
    val r = super[GroupResultsBase].checkInputDataTypes()
    if (r == TypeCheckResult.TypeCheckSuccess && (!impl._3))
      TypeCheckResult.TypeCheckFailure("group_results only accepts the process lambda when using an array of engine " +
        "result (engine, folder, collector or a nested group_result with a payload)")
    else
      r
  }
}
