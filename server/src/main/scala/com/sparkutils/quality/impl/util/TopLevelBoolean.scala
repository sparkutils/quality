package com.sparkutils.quality.impl.util

import com.sparkutils.quality.{groupProcessorBucketSizeKey, groupProcessorPercentFilter}
import com.sparkutils.quality.impl.util.ExtraConfig.ConfigMapOps
import com.sparkutils.quality.impl.util.TopLevelBoolean.Differentiator
import com.sparkutils.quality.impl.{Group, Groups, Runner, Trigger, Triggers}
import org.apache.spark.sql.catalyst.expressions.{Abs, And, EqualTo, Expression, Literal, Murmur3Hash, Or, Remainder, StartsWith}
import org.apache.spark.sql.types.{BooleanType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable.ArrayBuffer
import scala.collection.{Set, mutable}

object TopLevelBoolean {

  def params(runner: Runner): (Int, Double) = {
    val targetBucket = runner.extraConfig.int(groupProcessorBucketSizeKey, 130)
    val targetFilter = runner.extraConfig.double(groupProcessorPercentFilter, 0.010)
    (targetBucket, targetFilter)
  }

  def addToMap[K,I](map: mutable.HashMap[K, ArrayBuffer[I]], k: K, i: I): Unit = {
    map.get(k).fold{
      val a = ArrayBuffer(i)
      map.put(k, a)
      a
    }{ s =>
      s += i
    }
  }

  def apply(expressions: Seq[Trigger], triggerPercentFilter: Double): (mutable.HashMap[Expression, (Set[Expression], ArrayBuffer[Trigger])], Expression => Option[(Int, Expression)]) = {
    val osubs = SubExprsFrom.apply(expressions.map(_.expression))

    val filterOut = {
      val t = ((expressions.size.toDouble / 100.toDouble) * triggerPercentFilter).toInt
      if (t == 0)
        1
      else
        t
    }

    val subs = (e: Expression) => osubs(e).filter(r => r._1 > filterOut)

    val hmap = new mutable.HashMap[Set[Expression], ArrayBuffer[Trigger]]()

    expressions.foreach {
      n =>

        val s = topLevelFrom(n.expression, i => subs(i).isDefined)
/*
        if (s.isEmpty && count < 10) {
          count += 1
          System.out.println(s"Got empty for ${n.expression.getClass.getName} - ${n.expression.toString()}")
        }
*/
        addToMap(hmap, s, n)
    }

    val r =
      hmap.map{
        case (k, v) =>
          (k match {
            case _ if k.size == 1 => k.head
            case _ => k.reduce(And(_,_))
          }, (k, v))
      }.filter(p => p._1.collectLeaves().size > 2)

    (r, subs)
  }

  def sorted(expressions: Seq[Trigger], filterPercentage: Double):
    (Seq[(Expression, (Set[Expression], ArrayBuffer[Trigger]))], Expression => Option[(Int, Expression)]) = {
    val (res,subs) = apply(expressions, filterPercentage)

    (res.toSeq.sortBy(_._1.collectLeaves().size).reverse, subs)
  }

  case class Grouper[T <: Differentiator[T]](diff: T) {
    type theType = T
    def merged(l: theType, r: theType): Differentiator[T] = l.merged(r)
    def completeMerge(t: Differentiator[_]): Differentiator[T] = t.completeMerge.asInstanceOf[T]
  }

  sealed trait Differentiator[T <: Differentiator[T]] {
    def bucketer(bucket: Int, bucketSize: Int): Expression

    // operates over the entire trigger rule to get a bucket
    def bucket(trigger: Expression, bucketSize: Int): Int

    /**
     * if diff is compatible to be merged with this differentiator then a new merged differentiator is returned
     */
    def merged(diff: T): T

    def group: Grouper[T]

    def completeMerge: T

    def groups(bucketed: Map[Int, ArrayBuffer[(Int, Trigger)]], numberOfBuckets: Int)(addSeen: ArrayBuffer[Trigger] => Seq[Trigger]): Groups =
      Groups(
        bucketed.foldLeft(Seq.empty[Group]){
          case (cur, (bucket, trips)) =>
            val bucketedExp = bucketer(bucket, numberOfBuckets)

            val corrected = addSeen(trips.map(_._2))
            cur :+ Group(bucketedExp, corrected.minBy(_.salience).salience, Triggers(corrected))
        }
      )
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

  case class EqualToDiff(operands: Set[Expression]) extends Differentiator[EqualToDiff] {

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

    /**
     * if diff is compatible to be merged with this differentiator then a new merged differentiator is returned
     */
    override def merged(diff: EqualToDiff): EqualToDiff = this

    override def group: Grouper[EqualToDiff] = Grouper(this)

    override def completeMerge: EqualToDiff = this
  }

  /**
   * Buckets based on string prefixes instead of hashes
   * @param operand
   * @param prefixes
   */
  case class StringPrefixes(operand: Expression, prefixes: Seq[String]) extends Differentiator[StringPrefixes] {

    override def bucketer(bucket: Int, bucketSize: Int): Expression =
      StartsWith(operand, Literal(prefixes(bucket)))

    override def bucket(trigger: Expression, bucketSize: Int): Int = {
      val str = (trigger match {
        case EqualTo(Literal(left, StringType), op) if op == operand => left
        case EqualTo(op, Literal(right, StringType)) if op == operand => right
      }).toString
      prefixes.zipWithIndex.maxBy{ p =>
        if (str.startsWith(p._1))
          str.size
        else
          0
      }
    }._2

    /**
     * if diff is compatible to be merged with this differentiator then a new merged differentiator is returned.
     * These are the values themselves not prefixes, completeMerge does prefixing
     */
    override def merged(diff: StringPrefixes): StringPrefixes =
      StringPrefixes(operand, (prefixes ++ diff.prefixes).distinct)

    override def group: Grouper[StringPrefixes] = Grouper(StringPrefixes(operand, Seq.empty))

    override def completeMerge: StringPrefixes = {
      val pt = new PrefixTrie[String]
      prefixes.foreach(p => pt.addOne((p, p)))
      this
    }
  }
  // code can't reach this yet
  // $COVERAGE-OFF$

  case class NoIdeaDiff(exprs: Seq[Expression]) extends Differentiator[NoIdeaDiff] {
    def bucketer(bucket: Int, bucketSize: Int) =
      exprs match {
        case s if s.size == 1 => s.head
        case _ => exprs.reduce(And)
      }

    override def bucket(trigger: Expression, bucketSize: Int): Int = 0

    /**
     * if diff is compatible to be merged with this differentiator then a new merged differentiator is returned
     */
    override def merged(diff: NoIdeaDiff): NoIdeaDiff = this

    override def group: Grouper[NoIdeaDiff] = Grouper(this)

    override def completeMerge: NoIdeaDiff = this
  }
  // $COVERAGE-ON$

  def differentiate(expressions: Set[Expression]): Differentiator[_] = expressions match {
    case s if s.forall {
      case e@ EqualTo(left: Literal, operand) => true
      case e@ EqualTo(operand, right: Literal) => true
      case e => false
    } && s.nonEmpty =>
      val operands = EqualToDiff.split(s.toSeq).map(_._2)
      lazy val (strtyp, str) = {
        (s.head match {
          case e@ EqualTo(Literal(left: UTF8String, StringType), operand) => (true, left.toString)
          case e@ EqualTo(operand, Literal(right: UTF8String, StringType)) => (true, right.toString)
          case _ => (false, "")
        })
      }
      //if (s.size == 1 && strtyp)
      //  StringPrefixes(operands.head, Seq(str))
     // else
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

    val trigger = 0.12

    while(!found) {
      //println(s"running bucket $bucketSize for min $min and max $max with res $resCount")
      val b = bucket(triggers = expressions, targetParams = (min, trigger))
      val bCount = b.maxBy(_.size).size + b.size
      val t = bucket(triggers = expressions, targetParams = (max, trigger))
      val tCount = t.maxBy(_.size).size + t.size
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

  def bucket(triggers: Seq[Trigger], targetParams: (Int, Double) = (130, 0.12)): Seq[Group] = {
    val expressions = MultiCommutativeOpOps.origin(triggers)
    val (orderedLarger, subs) = sorted(expressions, targetParams._2)
    val targetBucket = targetParams._1

//    System.out.println(s"bucket input had orderedLarger size of ${orderedLarger.size} ")

    // remove duplicates
    val seen = new mutable.HashSet[Expression]

    def addSeen(pop: Iterable[Trigger], groupParts: Set[Expression]) = {
      val newPopSeqs = pop.filterNot(p => seen(p.expression))
      seen.++=(newPopSeqs.map(_.expression))
      newPopSeqs.toSeq.map( t =>
        t.copy(expression = removeTopLevels(groupParts, t.expression)))
    }

    val topHitter =
      orderedLarger.foldLeft(Seq.empty[Group]) {
        case (cur, (sub, (groupParts, triggers))) =>

          if (triggers.size > targetBucket) {
            // should be the maximal list already as all elements are subexprs, what is left are differentiators

            val differentiatingBooleans = new mutable.HashMap[Differentiator[_], ArrayBuffer[Trigger]]()

            triggers.foreach {
              trigger =>
                val theseParts = differentiate(differentiateFrom(trigger.expression, i => subs(i).isEmpty))

                addToMap(differentiatingBooleans, theseParts, trigger)
            }

            val mergedDifferentiatingBooleans = differentiatingBooleans.groupBy(_._1.group).map{
              case (g, m) =>
                val p =
                  m.reduce{
                    (l, r) =>
                      g.merged(l._1.asInstanceOf[g.theType], r._1.asInstanceOf[g.theType]) ->
                        l._2.addAll(r._2).asInstanceOf[ArrayBuffer[Trigger]]
                  }

                (g.completeMerge(p._1), p._2)
            }

            //println(s"differentiating booleans from $sub for ${triggers.size} triggers of:")
            //differentiatingBooleans.keys.foreach(println)

            val newSeqs =
              mergedDifferentiatingBooleans.flatMap {
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

                  Seq(Group(sub, triggers.minBy(_.salience).salience,
                    differentiator.groups(bucketed, numberOfBuckets)(addSeen(_, groupParts))
                  ))
              }
            cur ++ newSeqs
          } else if (triggers.size > 4) { // TODO random number
            // very small groups are expensive and should fall to the true bucket
            // likely no benefit in reducing further
            val newTriggers = addSeen(triggers, groupParts)
            cur :+ Group(sub, newTriggers.minBy(_.salience).salience, Triggers(newTriggers))
          } else
            cur
      }

    val rest = expressions.filterNot(p => seen(p.expression))
    if (rest.isEmpty)
      topHitter
    else
      (topHitter :+ Group(Literal(true), rest.minBy(_.salience).salience, Triggers(rest))).filter(_.size > 0)
  }

  def differentiateFrom(e: Expression, p: Expression => Boolean): Set[Expression] = {
    from(e, differentiateFromParts, p)
  }

  def topLevelFrom(e: Expression, p: Expression => Boolean): Set[Expression] = {
    from(e, fromParts, p)
  }

  def differentiateFromParts(expression: Expression): Set[Expression] = expression match {
    case And(left, right) => differentiateFromParts(left) ++ differentiateFromParts(right)
    case e: EqualTo => Set(e)
    case _ => Set.empty
  }

  def fromParts(expression: Expression): Set[Expression] = expression match {
    // TODO: intentionally excluded from subexpression elimination
    case _: Or => Set.empty
    case And(left, right) => fromParts(left) ++ fromParts(right)
    case e: Expression if e.dataType == BooleanType =>
      Set(e)
    case _ => Set.empty
  }

  private def from(expression: Expression, t: Expression => Set[Expression], p: Expression => Boolean): Set[Expression] = {
    val resDiff = t(expression).filter(p)

    resDiff
  }

  def removeTopLevels(groupExpressions: Set[Expression], expression: Expression): Expression = {
    val T = Literal(true)
    val replaced = expression.transformUp{
      case e if groupExpressions.contains(e) => T
      case And(T, right) => right
      case And(left, T) => left
      case And(T, T) => T
    }
    replaced
  }

}