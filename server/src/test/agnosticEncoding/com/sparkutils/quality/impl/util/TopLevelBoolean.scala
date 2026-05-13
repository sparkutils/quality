package com.sparkutils.quality.impl.util

import com.sparkutils.quality.impl.util
import org.apache.spark.sql.catalyst.expressions.{Abs, And, CaseWhen, EqualTo, Expression, If, Literal, Murmur3Hash, Not, Or, Remainder}
import org.apache.spark.sql.types.BooleanType

import scala.annotation.tailrec
import scala.collection.mutable

case class Trigger(expression: Expression, index: Int, salience: Int)

case class Group(groupFilter: Expression, lowestSalience: Int, triggers: Seq[Trigger])

object TopLevelBoolean {

  def apply(expressions: Seq[Trigger], triggerPercentFilter: Double): (Map[Expression, Seq[Trigger]], Map[Expression, Int]) = {
    val subs = SubExprsFrom.apply(expressions.map(_.expression)).toMap

    (expressions.foldLeft(Map.empty[Expression, Seq[Trigger]]){
      (cur, n) =>
        val s = from(n.expression, subs, expressions.size, triggerPercentFilter)
        util.MapOps.MapOps(cur).updatedWithF(s) {
          case Some(s) => Some(s :+ n)
          case None => Some(Seq(n))
        }
    }.filter(p => p._1.collectLeaves().size > 2), subs)
  }

  def sorted(expressions: Seq[Trigger], triggerPercentFilter: Double): (Seq[(Expression, Seq[Trigger])], Map[Expression, Int]) = {
    val (res,subs) = apply(expressions, triggerPercentFilter)

    (res.toSeq.sortBy(_._1.collectLeaves().size).reverse, subs)
  }

  sealed trait Differentiator {
    def bucketer(bucket: Int, bucketSize: Int): Expression
    def bucket(trigger: Expression, bucketSize: Int): Int
  }

  case class EqualToDiff(operand: Expression) extends Differentiator {
    def bucketer(bucket: Int, bucketSize: Int) =
      EqualTo(Remainder(Abs(Murmur3Hash(Seq(operand), 42)), Literal(bucketSize)), Literal(bucket))

    override def bucket(trigger: Expression, bucketSize: Int): Int = {
      val expression = trigger.collectFirst{
        case e@ EqualTo(left: Literal, operand) => e
        case e@ EqualTo(operand, right: Literal) => e
      }.get

      expression match {
        case e@ EqualTo(left: Literal, right) => Abs(Murmur3Hash(Seq(left), 42)).eval().asInstanceOf[Int] % bucketSize
        case e@ EqualTo(left, right: Literal) => Abs(Murmur3Hash(Seq(right), 42)).eval().asInstanceOf[Int] % bucketSize
      }
    }
  }

  case class NoIdeaDiff(expr: Expression) extends Differentiator {
    def bucketer(bucket: Int, bucketSize: Int) = expr

    override def bucket(trigger: Expression, bucketSize: Int): Int = 0
  }

  def differentiate(expression: Expression): Differentiator = expression match {
    case e@ EqualTo(left: Literal, right) => EqualToDiff(right)
    case e@ EqualTo(left, right: Literal) => EqualToDiff(left)
    case _ =>
      println("didn't get an EqualTo in this test set that's strange")
      NoIdeaDiff(expression)
  }

  def bestFit(expressions: Seq[Trigger], triggerPercentFilter: Double): (Seq[Group], Int) = {
    var min = 30
    var max = 140

    var step = 10

    var found = false
    var res: Seq[Group] = Seq.empty
    var resCount = Integer.MAX_VALUE
    var bucketSize = 0

    while(!found) {
      println(s"running bucket $bucketSize for min $min and max $max with res $resCount")
      val b = bucket(expressions = expressions, targetBucket = min, triggerPercentFilter)
      val bCount = b.maxBy(_.triggers.size).triggers.size + b.size
      val t = bucket(expressions = expressions, targetBucket = max, triggerPercentFilter)
      val tCount = t.maxBy(_.triggers.size).triggers.size + t.size
      res =
        if (tCount <= bCount)
          if (tCount <= resCount) {
            resCount = tCount
            bucketSize = max

            min += step
            t
          } else {
            min += step
            max -= step
            res
          }
        else
          if (bCount <= resCount) {
            resCount = bCount
            bucketSize = min

            max -= step
            b
          } else {
            min += step
            max -= step
            res
          }
      println(s"ran - new counts - bucket $bucketSize for min $min and max $max with res $resCount")
      if (max - min <= step) {
        step = 1
      }
/*
      var adjustedMin = false
      var adjustedMax = false

      if (min > bucketSize) {
        min = bucketSize
        adjustedMin = true
        if (max > bucketSize) {
          adjustedMax = true
          max -= step
        }
      }

      if ((max < bucketSize) && !adjustedMax) {
        max = bucketSize
        if ((min < bucketSize) && !adjustedMin) {
          min += step
        }
      }
*/
      if (max <= min) {
        found = true
      }
    }

    println(s"'optimal' bucket size was $bucketSize")

    (res, bucketSize)
  }

  def bucket(expressions: Seq[Trigger], targetBucket: Int = 130, triggerPercentFilter: Double = 0.12): Seq[Group] = {
    val (orderedLarger, subs) = sorted(expressions, triggerPercentFilter)
    // remove duplicates

    val seen = new mutable.HashSet[Expression]

    def addSeen(pop: Seq[Trigger]) = {
      val newPopSeqs = pop.filterNot(p => seen(p.expression))
      seen.++=(newPopSeqs.map(_.expression))
      newPopSeqs
    }

    val topHitter =
      orderedLarger.foldLeft(Seq.empty[Group]) {
        case (cur, (sub, triggers)) =>

          if (triggers.size > targetBucket) {
            // should be the maximal list already as all elements are subexprs, what is left are differentiators

            val differentiatingBooleans =
              triggers.foldLeft(Map.empty[Set[Differentiator], Seq[Trigger]]){
                case (map, trigger) =>
                  val theseParts = fromParts(trigger.expression).filterNot(i => subs.contains(i)).map(differentiate)

                  util.MapOps.MapOps(map).updatedWithF(theseParts) {
                      case Some(s) => Some(s :+ trigger)
                      case None => Some(Seq(trigger))
                  }
              }

            println(s"differentiating booleans from $sub for ${triggers.size} triggers of:")
            differentiatingBooleans.keys.foreach(println)

            val newSeqs =
              differentiatingBooleans.flatMap {
                case (differentiators, triggers) =>

                  val numberOfBuckets =
                    if (triggers.size % targetBucket == 0)
                      triggers.size / targetBucket
                    else
                      (triggers.size / targetBucket + 1)

                  println(s"target number of buckets $numberOfBuckets for ${triggers.size} for $differentiators")

                  val bucketed =
                    triggers.map{
                      t =>
                        differentiators.map(d => d -> d.bucket(t.expression, numberOfBuckets)) -> t
                    }.groupBy(_._1)

                  bucketed.foldLeft(Seq.empty[Group]){
                    case (cur, (bucket, trips)) =>
                      val bucketers = bucket.map(p => p._1.bucketer(p._2, numberOfBuckets))
                      val bucketedAnd = And(sub,
                        if (bucketers.size == 1)
                          bucketers.head
                        else
                          bucketers.reduce(And)
                      )

                      val corrected = addSeen(trips.map(_._2))
                      cur :+ Group(bucketedAnd, corrected.minBy(_.salience).salience, corrected)
                  }
              }
            cur ++ newSeqs
          } else {
            // likely no benefit in reducing further
            val newTriggers = addSeen(triggers)
            cur :+ Group(sub, newTriggers.minBy(_.salience).salience, newTriggers)
          }
      }

    val rest = expressions.filterNot(p => seen(p.expression))
    (topHitter :+ Group(Literal(true), rest.minBy(_.salience).salience, rest)).filter(_.triggers.nonEmpty)
  }

  def from(expression: Expression, subExprs: Map[Expression, Int], populationSize: Int, triggerPercentFilter: Double): Expression = {
    val filterOut = ((populationSize.toDouble / 100.toDouble) * triggerPercentFilter).toInt
    // anything that hits more than x % is not useful to group with, any top bool that only applies to one trigger is
    // equally useless
    val res = fromParts(expression).filter(e => subExprs.get(e).exists(_ < filterOut))
    res match {
      case Seq(head: Expression) => head
      case _ if res.isEmpty  => expression
      case _ => res.reduce(And(_,_))
    }
  }

  def fromParts(expression: Expression): Set[Expression] = expression match {
    /*case And(left: And, right: And) => fromParts(left) ++ fromParts(right)
    case And(left, right: And) => fromParts(left) ++ fromParts(right)
    case And(left: And, right) => fromParts(left) ++ fromParts(right)
    case a@ And(left, right) => Set(a) ++ fromParts(left) ++ fromParts(right)*/
    case CaseWhen(branches, elseValue) =>
      val plentyOfParts = for {
        (whenExpr, thenExpr) <- branches
        expr <- And(whenExpr, thenExpr)
      } yield expr

      (plentyOfParts ++ elseValue.toSeq).toSet
    case If(condition, trueValue, falseValue) => Set(And(condition, trueValue), And(Not(condition), falseValue))
    case Or(left, right) => Set(And(Not(left), right), And(left, Not(right)))
    case And(left, right) => fromParts(left) ++ fromParts(right)
    case e: Expression if e.dataType == BooleanType => Set(e)
    case _ => Set.empty
  }

}
