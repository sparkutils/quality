package com.sparkutils.quality

import com.sparkutils.quality.utils.{BloomLookupType, ExpressionLookupResult, MapLookupType}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
import org.apache.spark.sql.catalyst.expressions.{Expression, LeafExpression, Literal}
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

import scala.annotation.tailrec

/**
 * Allow 2.4 and 3 to co-exist
 */
object LookupIdFunctionImpl {


  /**
   * Note these only work on unresolved functions - actual names and expressions do not exist before analysis
   * @param exp
   * @return
   */
  def identifyLookups(exp: Expression): Option[ExpressionLookupResult] = {

    def children(res: Option[ExpressionLookupResult], children: Seq[Expression]): Option[ExpressionLookupResult] =
      children.foldLeft(res){
        (curRes, exp) =>
          accumulate(curRes, exp)
      }

    @tailrec
    def accumulate(res: Option[ExpressionLookupResult], exp: Expression): Option[ExpressionLookupResult] =
      exp match {
        // unresolved case where we cannot see more unresolved functions
        case UnresolvedFunction(FunctionIdentifier(funcName, None), Seq(Literal(name: UTF8String, StringType), next), _, _) if funcName.toLowerCase == "maplookup" || funcName.toLowerCase =="mapcontains" =>
          accumulate(res.map(r => r.copy(constants = r.constants + MapLookupType(name.toString))).orElse(Some(ExpressionLookupResult(Set(MapLookupType(name.toString)), false))), next)
        // unresolved case where we have constants - it'll fail  at creation
        case UnresolvedFunction(FunctionIdentifier(funcName, None), Seq(exp, next), _, _) if funcName.toLowerCase == "maplookup" || funcName.toLowerCase =="mapcontains" =>
          accumulate(res.map(r => r.copy(hasExpressionLookups = true)).orElse(Some(ExpressionLookupResult(Set.empty, true))), next)

        // unresolved case where we cannot see more unresolved functions
        case UnresolvedFunction(FunctionIdentifier(funcName, None), Seq(next, Literal(name: UTF8String, StringType)), _, _) if funcName.toLowerCase == "probabilityin" || funcName.toLowerCase =="saferrowid" =>
          accumulate(res.map(r => r.copy(constants = r.constants + BloomLookupType(name.toString))).orElse(Some(ExpressionLookupResult(Set(BloomLookupType(name.toString)), false))), next)
        // unresolved case where we have constants - it'll fail  at creation
        case UnresolvedFunction(FunctionIdentifier(funcName, None), Seq(next, exp), _, _) if funcName.toLowerCase == "probabilityin" || funcName.toLowerCase =="saferrowid" =>
          accumulate(res.map(r => r.copy(hasExpressionLookups = true)).orElse(Some(ExpressionLookupResult(Set.empty, true))), next)

        case _ : LeafExpression => res
        case parent: Expression => children(res, parent.children)
      }

    accumulate(None, exp)
  }
}
