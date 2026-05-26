package com.sparkutils.quality.impl.util

import com.sparkutils.quality.groupProcessorBucketSizeKey
import com.sparkutils.quality.impl.{Group, Runner, Trigger, Triggers, util}
import org.apache.spark.sql.catalyst.expressions.{Abs, And, EqualTo, Expression, Literal, Murmur3Hash, Remainder}

import scala.collection.{Set, mutable}
import scala.util.Try

object TopLevelBoolean {

  def params(runner: Runner): Int = {
    val targetBucket = Try(Triggers.getValue(groupProcessorBucketSizeKey, runner.extraConfig, "130").toInt).
      getOrElse(130)
    targetBucket
  }

  def apply(expressions: Seq[Trigger]): (Map[Expression, Seq[Trigger]], Expression => Option[(Int, Expression)]) = {
    val osubs = SubExprsFrom.apply(expressions.map(_.expression))

    val subs = (e: Expression) => osubs(e).filter(r => r._1 > 1)

 //   var count = 0

    def group() = expressions.foldLeft(Map.empty[Set[Expression], Seq[Trigger]]){
      (cur, n) =>

        val s = from(n.expression, subs)
/*
        if (s.isEmpty && count < 10) {
          count += 1
          System.out.println(s"Got empty for ${n.expression.getClass.getName} - ${n.expression.toString()}")
        }
*/
        util.MapOps.MapOps(cur).updatedWithF(s) {
          case Some(s) => Some(s :+ n)
          case None => Some(Seq(n))
        }
    }.map{
      case (k, v) =>
        (k match {
          case _ if k.size == 1 => k.head
          //case _ if k.isEmpty => Literal(true) // happens on Databricks
          case _ => k.reduce(And(_,_))
        }, v)
    }.filter(p => p._1.collectLeaves().size > 2)

    (group() : Map[Expression, Seq[Trigger]], subs)
  }

  def sorted(expressions: Seq[Trigger]): (Seq[(Expression, Seq[Trigger])], Expression => Option[(Int, Expression)]) = {
    val (res,subs) = apply(expressions)

    (res.toSeq.sortBy(_._1.collectLeaves().size).reverse, subs)
  }

  sealed trait Differentiator {
    def bucketer(bucket: Int, bucketSize: Int): Expression

    // operates over the entire trigger rule to get a bucket
    def bucket(trigger: Expression, bucketSize: Int): Int
  }

  object EqualToDiff {

    protected[quality] def split(exprs: Seq[Expression]): Seq[(Literal, Expression)] = {

      exprs.flatMap {
        case e@EqualTo(left: Literal, operand) => Some(left -> operand)
        case e@EqualTo(operand, right: Literal) => Some(right -> operand)
        case _ => None
      }.sortBy(_._1.hashCode())
    }
  }

  case class EqualToDiff(operands: Set[Expression]) extends Differentiator {

    def bucketer(bucket: Int, bucketSize: Int) =
      EqualTo(new Remainder(Abs(Murmur3Hash(operands.toSeq.sortBy(_.hashCode()), 42)), Literal(bucketSize)), Literal(bucket))

    override def bucket(trigger: Expression, bucketSize: Int): Int = {
      val pairs =
        EqualToDiff.split(trigger.collect{ case e: EqualTo => e} ++ (
          if (trigger.isInstanceOf[EqualTo])
            Set(trigger)
          else
            Set.empty
        ).toSeq)

      val lits = pairs.filter(p => operands.contains(p._2)).sortBy(_._2.hashCode()).map(_._1)

      Abs(Murmur3Hash(lits, 42)).eval().asInstanceOf[Int] % bucketSize
    }
  }
  // code can't reach this yet
  // $COVERAGE-OFF$

  case class NoIdeaDiff(exprs: Seq[Expression]) extends Differentiator {
    def bucketer(bucket: Int, bucketSize: Int) =
      exprs match {
        case s if s.size == 1 => s.head
        case _ => exprs.reduce(And)
      }

    override def bucket(trigger: Expression, bucketSize: Int): Int = 0
  }
  // $COVERAGE-ON$

  def differentiate(expressions: Set[Expression]): Differentiator = expressions match {
    case s if s.forall {
      case e@ EqualTo(left: Literal, operand) => true
      case e@ EqualTo(operand, right: Literal) => true
      case e => false
    } && s.nonEmpty =>
      val operands = EqualToDiff.split(s.toSeq).map(_._2)
      EqualToDiff(operands.toSet) //TODO - and then for `a = `b tests can we simplify?
    case _ =>
      System.out.println(s"didn't get an EqualTo in this test set that's strange got $expressions")
      NoIdeaDiff(expressions.toSeq)
  }

  // too memory intensive for CI
  // $COVERAGE-OFF$

  def bestFit(expressions: Seq[Trigger]): (Seq[Group], Int) = {
    var min = 100
    var max = 200

    var step = 10

    var found = false
    var res: Seq[Group] = Seq.empty
    var resCount = Integer.MAX_VALUE
    var bucketSize = 0

    while(!found) {
      //println(s"running bucket $bucketSize for min $min and max $max with res $resCount")
      val b = bucket(triggers = expressions, targetBucket = min)
      val bCount = b.maxBy(_.triggers.size).triggers.size + b.size
      val t = bucket(triggers = expressions, targetBucket = max)
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
      //println(s"ran - new counts - bucket $bucketSize for min $min and max $max with res $resCount")
      if (max - min <= step) {
        step = 1
      }

      if (max <= min) {
        found = true
      }
    }

    //println(s"'optimal' bucket size was $bucketSize")

    (res, bucketSize)
  }
  // $COVERAGE-ON$

  def bucket(triggers: Seq[Trigger], targetBucket: Int = 130): Seq[Group] = {
    val expressions = MultiCommutativeOpOps.origin(triggers)
    val (orderedLarger, subs) = sorted(expressions)

//    System.out.println(s"bucket input had orderedLarger size of ${orderedLarger.size} ")

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
              triggers.foldLeft(Map.empty[Differentiator, Seq[Trigger]]){
                case (map, trigger) =>
                  val theseParts = differentiate(fromParts(trigger.expression).filterNot{
                    i =>
                      subs(i).isDefined
                  })

                  util.MapOps.MapOps(map).updatedWithF(theseParts) {
                      case Some(s) => Some(s :+ trigger)
                      case None => Some(Seq(trigger))
                  }
              }

            //println(s"differentiating booleans from $sub for ${triggers.size} triggers of:")
            //differentiatingBooleans.keys.foreach(println)

            val newSeqs =
              differentiatingBooleans.flatMap {
                case (differentiator, triggers) =>

                  val numberOfBuckets =
                    if (triggers.size % targetBucket == 0)
                      triggers.size / targetBucket
                    else
                      (triggers.size / targetBucket + 1)

                  //println(s"target number of buckets $numberOfBuckets for ${triggers.size} for $differentiator")

                  val bucketed =
                    triggers.map{
                      t =>
                        differentiator.bucket(t.expression, numberOfBuckets) -> t
                    }.groupBy(_._1)

                  bucketed.foldLeft(Seq.empty[Group]){
                    case (cur, (bucket, trips)) =>
                      val bucketedExp = differentiator.bucketer(bucket, numberOfBuckets)

                      val corrected = addSeen(trips.map(_._2))
                      cur :+ Group(And(bucketedExp, sub), corrected.minBy(_.salience).salience, corrected)
                  }
              }
            cur ++ newSeqs
          } else if (triggers.size > 4) { // TODO random number
            // very small groups are expensive and should fall to the true bucket
            // likely no benefit in reducing further
            val newTriggers = addSeen(triggers)
            cur :+ Group(sub, newTriggers.minBy(_.salience).salience, newTriggers)
          } else
            cur
      }

    val rest = expressions.filterNot(p => seen(p.expression))
    if (rest.isEmpty)
      topHitter
    else
      (topHitter :+ Group(Literal(true), rest.minBy(_.salience).salience, rest)).filter(_.triggers.nonEmpty)
  }

  def from(expression: Expression, subExprs: Expression => Option[(Int, Expression)]): Set[Expression] = {
    val res = fromParts(expression).filter {e =>
      subExprs(e).isDefined
    }
    res
  }

  def fromParts(expression: Expression): Set[Expression] = expression match {
    case And(left, right) => fromParts(left) ++ fromParts(right)
    case e: EqualTo => Set(e)
    case _ => Set.empty
  }

}
