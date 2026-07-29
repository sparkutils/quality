package com.sparkutils.quality.impl.util

import com.sparkutils.quality.impl.{DefaultTriggerGrouper, Runner, Trigger, TriggerResult, Triggers}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Expression, GenericInternalRow, Literal}
import org.apache.spark.sql.catalyst.expressions.codegen.{Block, CodeAndComment, CodegenContext, EmptyBlock, ExprCode, VariableValue}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}

import java.util.UUID
import scala.collection.mutable

object SwitchGroups {

  private val uuid = UUID.randomUUID().toString

  val assumedSize = 4

  def groups(triggers: Seq[Trigger]): Option[SwitchGroups] = {
    val operands = mutable.Set.empty[Expression]
    val types = mutable.Set.empty[DataType]

    val labelsAndTrigger = triggers.map( trigger => trigger.expression match {
      case EqualTo(Literal(left, t), op) =>
        operands.add(op)
        types.add(t)
        (left, trigger)
      case EqualTo(op, Literal(right, t)) =>
        types.add(t)
        operands.add(op)
        (right, trigger)
      case _ =>
        (uuid, trigger)
    } )

    def groupedAndSorted(triggers: Seq[(String, Trigger)]): Seq[(String, Seq[Trigger])] = {
      val gr = triggers.groupBy(_._1).map(p => p._1 -> p._2.map(_._2)).toSeq

      gr.sortBy(_._1)
    }

    if ((operands.size > 1) || (types.size > 1) || labelsAndTrigger.exists(_._1 == uuid))
      None
    else types.head match {
      case StringType =>
        Some(SwitchGroups(operands.head, "String", v => s"$v.toString()", lessThan = (s, r) => s"$s.compareTo($r) < 0",
          lessThanOrEqual = (s, l) => s"$s.compareTo($l) <= 0",
          greaterThanOrEqual = (s, l) => s"$s.compareTo($l) >= 0", zero = "\"\"",
          lessThanSpark = (s, l) => s"$s.${UTF8StringOps.compareTo}($l) < 0",
          greaterThanSpark = (s, r) => s"$s.${UTF8StringOps.compareTo}($r) > 0",
          sparkType = "org.apache.spark.unsafe.types.UTF8String",
          initSpark = (v, t) => s"$v = org.apache.spark.unsafe.types.UTF8String.fromString($t);",
          triggers = groupedAndSorted(labelsAndTrigger.map(p => s""""${p._1.toString}"""" -> p._2))
        ))

      case IntegerType =>
        Some(SwitchGroups(operands.head, "int", v => s"$v", lessThan = (s, r) => s"$s < $r",
          lessThanOrEqual = (s, l) => s"$s <= $l",
          greaterThanOrEqual = (s, l) => s"$s >= $l", zero = "0",
          lessThanSpark = (s, l) => s"$s < $l",
          greaterThanSpark = (s, r) => s"$s > $r",
          sparkType = "int",
          initSpark = (v, t) => s"$v = $t;",
          triggers = groupedAndSorted(labelsAndTrigger.map(p => p._1.toString -> p._2))
        ))

      case _ => None
    }

  }
}

/**
 * Produces grouping via a switch, unlike normal grouping the group itself is the separate compilation and
 * each sub SwitchGroup is directly inlined if there is only one trigger per group
 *
 * @param groups
 */
case class SwitchGroups(groupingExpression: Expression, typ: String,
                        conversion: String => String = identity, lessThan: (String, String) => String,
                        lessThanOrEqual: (String, String) => String,
                        greaterThanOrEqual: (String, String) => String, zero: String,
                        lessThanSpark: (String, String) => String,
                        greaterThanSpark: (String, String) => String,
                        sparkType: String, initSpark: (String, String) => String,
                        triggers: Seq[(String, Seq[Trigger])]
                        ) extends Logging {

  def produceGroup(ctx: CodegenContext, prefix: String,
                   groupDepth: Int,
                   map: Map[Int, (CodegenContext, ParameterInformation, Expression, Boolean) => Block],
                   params: ParameterInformation): TriggerResult = {

    val groupCalls: Seq[(String, Block)] =
      triggers.map {
        p =>
          val mult = p._2.groupBy(_.salience).filter(_._2.size > 1)
          // $COVERAGE-OFF$ // zero point in testing and no different than any other salience usage
          if (p._2.size > 1 && mult.nonEmpty) {
            logDebug(s"SwitchGroups detected random outcomes: multiple Triggers with the same salience values: $mult")
          }
          // $COVERAGE-ON$

          p._1 -> {
            val t = p._2.minBy(_.salience)
            map(t.index)(ctx, params, t.expression, true) // alreadyPassed
          }
      }

    TriggerResult( Seq(generateSwitch(ctx, prefix, groupDepth, params, groupCalls)).iterator, "", Seq.empty, params)
  }

  private def generateSwitch(ctx: CodegenContext, prefix: String, groupDepth: Int, params: ParameterInformation,
                             groupCalls: Seq[(String, Block)]) = {
    val exprFuncName = ctx.freshName(prefix + "GEFuncSwitchGroups" + groupDepth)

    val (min, max) = (groupCalls.head._1, groupCalls.last._1)

    val cases = groupCalls.map {
      case (bucket, codeToRun) =>
        val exprFuncName = ctx.freshName(prefix + "callGEFuncSwitchGroup" + groupDepth)
        val callFun = ctx.addNewFunction(exprFuncName,
          code"""
           private void $exprFuncName(${params.paramsDef}) {
             $codeToRun
           }
          """.code
        )
        bucket -> code"""
          case $bucket:
            $callFun(${params.paramsCall});
            break;
        """
    }

    val expr = groupingExpression.genCode(ctx)

    def buildSwitch(switchVal: String, cases: Seq[(String,Block)], default: String, single: Boolean): String = {
      val folded = cases.foldLeft(code"") {
        case (cur, n) =>
          code"""
          $cur
          ${n._2}
        """
      }

      val switchIf: String => String =
        if (single)
          identity
        else
        (cases.headOption.map( v => greaterThanOrEqual(switchVal, v._1)),
          cases.lastOption.map( v => lessThanOrEqual(switchVal, v._1))) match {
          case (Some(min), Some(max)) =>
            s => s"""
                if (($min) && ($max)) {
                  $s
                } else {
                  $default
                }
              """
          case (Some(min), None) =>
            s => s"""
                if ($min) {
                  $s
                } else {
                  $default
                }
              """
          case (None, Some(max)) =>
            s => s"""
                if ($max) {
                  $s
                } else {
                  $default
                }
              """
        }


      switchIf(s"""
        switch($switchVal) {
          $folded
        }
         """)
    }

    def buildSwitches(switchVal: String, chunked: Seq[Seq[(String,Block)]]): String = {
      if (chunked.size == 1) {
        buildSwitch(switchVal, chunked.head, "", true)
      } else {
        // we have more chunks
        val head = chunked.head
        val (restLeft, restRight) = chunked.tail.splitAt((chunked.size / 2) - 1)

        def headVal(chunked: Seq[Seq[(String,Block)]]): String =
          (for {
            first <- chunked.headOption
            second <- first.headOption
          } yield second._1).getOrElse(zero)

        val (restLeftStartBucket, restRightStartBucket) = (headVal(restLeft), headVal(restRight))

        def switches(chunked: Seq[Seq[(String,Block)]]): Option[String] =
          if (chunked.isEmpty)
            None
          else
            Some(buildSwitches(switchVal, chunked))

        val (switchLeft, switchRight) = (switches(restLeft), switches(restRight))

        val (paramsDef, paramsCall) = {
          val switchTypeVal = s"$typ $switchVal"
          if (params.paramsDef.contains(switchTypeVal))
            (params.paramsDef, params.paramsCall) // for example if it's directly an inputadapter_value e.g. primitive (booleangroupertest)
          else
            (s"$switchTypeVal, ${params.paramsDef}", s"$switchVal, ${params.paramsCall}")
        }

        def switch(switch: String, lOrR: String) = {
          val exprFuncName = ctx.freshName(prefix + s"GEFuncSwitchGroupsNested${lOrR}_" + groupDepth)

          val callFun = ctx.addNewFunction(exprFuncName,
            code"""
             private void $exprFuncName($paramsDef) {
               $switch
             }
            """.code
          )
          callFun
        }

        val (leftFun, rightFun) = (switchLeft.map(switch(_, "L")), switchRight.map(switch(_, "R")))

        val (geLeft, lessThanRight) =
          (greaterThanOrEqual(switchVal, restLeftStartBucket), lessThan(switchVal, restRightStartBucket))

        val defalt =
          (leftFun, rightFun) match {
            case (Some(leftFun), Some(rightFun)) =>
              s"""
                if (($geLeft) && ($lessThanRight)) {
                  $leftFun($paramsCall);
                } else {
                  $rightFun($paramsCall);
                }
              """
            case (Some(leftFun), None) =>
              s"""
                if ($geLeft) {
                  $leftFun($paramsCall);
                }
              """
            case (None, Some(rightFun)) =>
              s"""
                if (${greaterThanOrEqual(switchVal, restRightStartBucket)}) {
                  $rightFun($paramsCall);
                }
              """
          }

        val r = buildSwitch(switchVal, head, defalt, false)

        r
      }
    }

    val minSpark = ctx.addMutableState(sparkType, "minSpark", initFunc = initSpark(_,min))
    val maxSpark = ctx.addMutableState(sparkType, "maxSpark", initFunc = initSpark(_,max))

    val converted = conversion(expr.value)
    val (converting, switchName) =
      if (converted.isEmpty || converted == expr.value.code)
        ("", expr.value.code)
      else {
        val fresh = ctx.freshName("converted")
        (s"$typ $fresh = $converted;", fresh)
      }

    val groupSize = 200
    val rangeCheck =
      if (cases.size > groupSize)
        s"|| (${lessThanSpark(expr.value, minSpark)}) || (${greaterThanSpark(expr.value, maxSpark)})"
      else
        ""

    ctx.addNewFunction(exprFuncName,
      code"""
       private void $exprFuncName(${params.paramsDef}) {
         ${expr.code}
         if (${expr.isNull} $rangeCheck) {
         } else {
           $converting

           ${
            buildSwitches(switchName, /// TODO uses Spark mechanism to group, this is for PoC
              cases.grouped(groupSize).toSeq)
          }
         }
       }
      """.code
    )
  }
}
