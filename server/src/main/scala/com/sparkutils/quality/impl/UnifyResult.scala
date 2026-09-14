package com.sparkutils.quality.impl

import com.sparkutils.quality.QualityException.qualityException
import com.sparkutils.quality.impl.UnifyResult.{rd, rdOpt, typeCheckText}
import com.sparkutils.quality.impl.util.Compare
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression, UnaryExpression}
import org.apache.spark.sql.types.{DataType, StructType}

object UnifyResult {

  def rdOpt(child: Expression): Option[(DataType, ((Int, DataType), (Int, DataType)))] =
    child.dataType match {
      case s: StructType =>
        val fields = s.fields.zipWithIndex

        (
          for {
            r <- fields.find(_._1.name == "result")
            rsr <- fields.find{ p =>
              val f = p._1
              ( f.name == "ruleSuiteGroup" &&
                Compare.equalsIgnoreCaseAndNullability(f.dataType, Encoders.ruleSuiteGroupResultsTypedEnc.catalystRepr) ) ||
                ( f.name == "ruleSuiteResults" &&
                  Compare.equalsIgnoreCaseAndNullability(f.dataType, com.sparkutils.quality.impl.types.ruleSuiteResultType) )
            }
          } yield
            (
              StructType(Seq(
                rsr._1, r._1
              )),
              ((rsr._2,rsr._1.dataType), (r._2,r._1.dataType))
            )
          )
      case _ =>
        None
    }

  def rd(child: Expression): (DataType, ((Int, DataType), (Int, DataType))) =
   rdOpt(child).getOrElse(qualityException(typeCheckText(child.dataType)))

  def typeCheckText(typ: DataType) = s"UnifyResult supports non-debug engine results and (ruleSuiteGroup: " +
    s"RuleSuiteGroup, result: ) pairs, instead $typ was provided"
}

case class UnifyResult(child: Expression) extends UnaryExpression with CodegenFallback with NonSQLExpression {

  override def checkInputDataTypes(): TypeCheckResult = {
    if (rdOpt(child).isDefined)
      TypeCheckResult.TypeCheckSuccess
    else
      TypeCheckResult.TypeCheckFailure(typeCheckText(child.dataType))
  }

  override def nullable: Boolean = false

  lazy val ((a, at), (b, bt)) = rd(child)._2

  override def eval(input: InternalRow): Any = {
    val r = child.eval(input).asInstanceOf[InternalRow]
    InternalRow(r.get(a, at), r.get(b, bt))
  }

  override def dataType: DataType = rd(child)._1

  protected def withNewChildInternal(newChild: Expression): Expression = copy(newChild)
}
