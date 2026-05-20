package com.sparkutils.quality.impl.util

import com.sparkutils.quality.impl.{Group, Trigger, util}
import org.apache.spark.sql.catalyst.expressions.{Abs, And, EqualTo, Expression, Literal, Murmur3Hash, Remainder}

import scala.collection.mutable

object TopLevelBoolean {

  def apply(expressions: Seq[Trigger], triggerPercentFilter: Double): (Map[Expression, Seq[Trigger]], Map[Expression, Int]) = {
    val subs = SubExprsFrom.apply(expressions.map(_.expression)).toMap

    val filterOut = ((expressions.size.toDouble / 100.toDouble) * triggerPercentFilter).toInt

    (expressions.foldLeft(Map.empty[Expression, Seq[Trigger]]){
      (cur, n) =>
        val s = from(n.expression, subs, filterOut)
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

    // operates over the entire trigger rule to get a bucket
    def bucket(trigger: Expression, bucketSize: Int): Int
  }

  object EqualToDiff {

    protected[quality] def split(exprs: Seq[Expression]) = {

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
    } =>
      val operands = EqualToDiff.split(s.toSeq).map(_._2)
      EqualToDiff(operands.toSet) //TODO - and then for `a = `b tests can we simplify?
    case _ =>
      //println("didn't get an EqualTo in this test set that's strange")
      NoIdeaDiff(expressions.toSeq)
  }

  // too memory intensive for CI
  // $COVERAGE-OFF$

  def bestFit(expressions: Seq[Trigger], triggerPercentFilter: Double): (Seq[Group], Int) = {
    var min = 30
    var max = 140

    var step = 10

    var found = false
    var res: Seq[Group] = Seq.empty
    var resCount = Integer.MAX_VALUE
    var bucketSize = 0

    while(!found) {
      //println(s"running bucket $bucketSize for min $min and max $max with res $resCount")
      val b = bucket(triggers = expressions, targetBucket = min, triggerPercentFilter)
      val bCount = b.maxBy(_.triggers.size).triggers.size + b.size
      val t = bucket(triggers = expressions, targetBucket = max, triggerPercentFilter)
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

  def bucket(triggers: Seq[Trigger], targetBucket: Int = 130, triggerPercentFilter: Double = 0.12): Seq[Group] = {
    val expressions = MultiCommutativeOpOps.origin(triggers)
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
              triggers.foldLeft(Map.empty[Differentiator, Seq[Trigger]]){
                case (map, trigger) =>
                  val theseParts = differentiate(fromParts(trigger.expression).filterNot(i => subs.contains(i)))

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

  def from(expression: Expression, subExprs: Map[Expression, Int], filterOut: Double): Expression = {

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
    case And(left, right) => fromParts(left) ++ fromParts(right)
    case e: EqualTo => Set(e)
    //case e: Expression if e.dataType == BooleanType => Set(e)
    case _ => Set.empty
  }

}
