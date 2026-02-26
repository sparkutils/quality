package org.apache.spark.sql.qualityFunctions

import com.sparkutils.quality.impl.GroupResultsBase
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, HigherOrderFunction, NamedLambdaVariable}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataType

case class GroupResultsWithProcess(arguments: Seq[Expression], function: Expression) extends GroupResultsBase
  with Binder {

  lazy val ref = arguments.last.asInstanceOf[RefExpression]

  override def processResultType: DataType = function.dataType
  override def processResultNullable: Boolean = function.nullable

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
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
}
