package com.sparkutils.quality.impl.util

import com.sparkutils.quality.{groupProcessorAuditBucketStep, groupProcessorAuditMaxBucket, groupProcessorAuditMinBucket, groupProcessorBucketSizeKey, groupProcessorPercentFilter}
import com.sparkutils.quality.impl.util.ExtraConfig.ConfigMapOps
import com.sparkutils.quality.impl.{Group, Groups, IfRelevantExpr, Runner, Trigger, Triggers}
import com.sparkutils.quality.impl.imports.ClassicRuleResultsImports.IgnoredRuleExpr
import org.apache.spark.sql.catalyst.expressions.{Abs, And, EqualTo, Expression, If, Literal, Murmur3Hash, Or, Remainder}
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType}

import scala.collection.mutable.ArrayBuffer
import scala.collection.{Set, mutable}

object TopLevelBoolean {

  def defaultPercentFilter = 0.010

  def params(runner: Runner): (Int, Double) = {
    val targetBucket = runner.extraConfig.int(groupProcessorBucketSizeKey, 130)
    val targetFilter = runner.extraConfig.double(groupProcessorPercentFilter, defaultPercentFilter)
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
    val osubs = SubExprs.apply(expressions.map(_.expression))

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
            case _ if k.isEmpty => Literal(true)
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

  // everything goes into the same bucket for switching on strings / ints
  case class SwitchDiff(operand: Expression) extends Differentiator {

    def bucketer(bucket: Int, bucketSize: Int) = Literal(true)

    override def bucket(trigger: Expression, bucketSize: Int): Int = 0
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

      if (operands.size == 1 && (operands.head.dataType == StringType || operands.head.dataType == IntegerType))
        SwitchDiff(operands.head) // TODO - duplicates, need to keep all to verify uniqueness and fallback!
      else
        EqualToDiff(operands.toSet) //TODO - and then for `a = `b tests can we simplify?
    case _ =>
      System.out.println(s"didn't get an EqualTo in this test set that's strange got $expressions")
      NoIdeaDiff(expressions.toSeq)
  }

  // too memory intensive for CI
  // $COVERAGE-OFF$

  case class BestFit(groups: Seq[Group], optimalBucketSize: Int, maxDeepestEvaluationSize: Int,
                     optimisedDeepestEvaluationSize: Int, rangeMin: Int, rangeMax: Int, percent: Double)

  def bestFit(expressions: Seq[Trigger], runner: Runner): BestFit = {
    val startingMin = runner.extraConfig.int(groupProcessorAuditMinBucket, 100)
    val startingMax = runner.extraConfig.int(groupProcessorAuditMaxBucket, 200)
    var min = startingMin
    var max = startingMax

    val triggerPercentFilter = runner.extraConfig.double(groupProcessorPercentFilter, defaultPercentFilter)

    var step = runner.extraConfig.int(groupProcessorAuditBucketStep, 10)

    var found = false
    var res: Seq[Group] = Seq.empty
    var resCount = Integer.MAX_VALUE
    var bucketSize = 0

    while(!found) {
      //println(s"running bucket $bucketSize for min $min and max $max with res $resCount")
      val b = bucket(triggers = expressions, targetParams = (min, triggerPercentFilter))
      val bCount = b.maxBy(_.optimisedSize).optimisedSize + b.size
      val t = bucket(triggers = expressions, targetParams = (max, triggerPercentFilter))
      val tCount = t.maxBy(_.optimisedSize).optimisedSize + t.size
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

    val optimal = bucket(triggers = expressions, targetParams = (bucketSize, triggerPercentFilter))
    BestFit(res, bucketSize,
      optimal.maxBy(_.size).size + optimal.size,
      optimal.maxBy(_.optimisedSize).optimisedSize + optimal.size,
      startingMin, startingMax, triggerPercentFilter)
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
        t.copy(expression = removeTopLevels(groupParts, t.expression))).sortBy(_.salience)
    }

    // must be called with the original triggers: removeTopLevels rewrites the matched filter to
    // Literal(true), after which the if_relevant / if shape is gone
    def filterFalseFor(pop: Iterable[Trigger], groupFilter: Expression): Seq[(Int, Expression)] =
      pop.toSeq.flatMap { t =>
        filterFalseResultFor(groupFilter, t.expression).map(t.index -> _)
      }

    val topHitter =
      orderedLarger.foldLeft(Seq.empty[Group]) {
        case (cur, (sub, (groupParts, triggers))) =>

          if (triggers.size > targetBucket) {
            // should be the maximal list already as all elements are subexprs, what is left are differentiators

            val differentiatingBooleans = new mutable.HashMap[Differentiator, ArrayBuffer[Trigger]]()

            triggers.foreach {
              trigger =>
                val theseParts = differentiate(differentiateFrom(trigger.expression, i => subs(i).isEmpty))

                addToMap(differentiatingBooleans, theseParts, trigger)
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

                  if (bucketed.size == 1 && differentiator.bucketer(0,numberOfBuckets) == Literal(true)) {
                    // if it's "true" lift it back up
                    val originals = bucketed.head._2.map(_._2)
                    val filterFalse = filterFalseFor(originals, sub)
                    val corrected = addSeen(originals, groupParts)
                    val lowest = corrected.minBy(_.salience).salience
                    Seq(Group(sub, lowest, Triggers(corrected, lowest), filterFalse))
                  } else {
                    // inner groups key on bucketedExp, so sub's filter-false results belong to
                    // the outer group
                    val outerFilterFalse = filterFalseFor(triggers, sub)
                    Seq(Group(sub, triggers.minBy(_.salience).salience, Groups(
                      bucketed.foldLeft(Seq.empty[Group]){
                        case (cur, (bucket, trips)) =>
                          val bucketedExp = differentiator.bucketer(bucket, numberOfBuckets)

                          val corrected = addSeen(trips.map(_._2), groupParts)
                          val lowest = corrected.minBy(_.salience).salience
                          cur :+ Group(bucketedExp, lowest, Triggers(corrected, lowest))
                      }.sortBy(_.lowestSalience)
                    ), outerFilterFalse))
                  }
              }
            cur ++ newSeqs
          } else if (triggers.size > 4) { // TODO random number
            // very small groups are expensive and should fall to the true bucket
            // likely no benefit in reducing further
            val filterFalse = filterFalseFor(triggers, sub)
            val newTriggers = addSeen(triggers, groupParts)
            val lowest = newTriggers.minBy(_.salience).salience
            cur :+ Group(sub, lowest, Triggers(newTriggers, lowest), filterFalse)
          } else
            cur
      }

    val rest = expressions.filterNot(p => seen(p.expression))
    (
      if (rest.isEmpty)
        topHitter
      else {
        val lowest = rest.minBy(_.salience).salience
        (topHitter :+ Group(Literal(true), lowest, Triggers(rest, lowest))).filter(_.size > 0)
      }
      ).sortBy(_.lowestSalience)
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
    // if_relevant is IntegerType, so the BooleanType case below never sees it.  Its filter is
    // only groupable because Group carries filterFalseResults, see filterFalseResultFor.
    // A nullable filter cannot be grouped: if_relevant answers Failed for a null filter but the
    // generated branch is `if ((!isNull) && value)`, which cannot tell null from false.
    case IfRelevantExpr(filter, _) if filter.dataType == BooleanType && !filter.nullable =>
      fromParts(filter)
    case If(predicate, trueValue, falseValue)
      if predicate.dataType == BooleanType &&
         trueValue.dataType == BooleanType &&
         falseValue.dataType == BooleanType =>
      fromParts(predicate)
    case e: Expression if e.dataType == BooleanType =>
      Set(e)
    case _ => Set.empty
  }

  /**
   * True when groupFilter is the whole of condition, or one of its top level And conjuncts.
   *
   * fromParts recurses into a composite filter and the frequency filter in `from` then keeps only
   * the shared conjuncts, so the group filter is regularly a strict subset of the condition it was
   * lifted out of.  A conjunct being false still makes the whole conjunction false, so the
   * filter-false answer is the same either way.
   */
  private def coversCondition(groupFilter: Expression, condition: Expression): Boolean =
    condition match {
      case _ if condition.semanticEquals(groupFilter) => true
      case And(left, right) => coversCondition(groupFilter, left) || coversCondition(groupFilter, right)
      case _ => false
    }

  /**
   * The result a trigger must produce when a grouped filter is false, None where the runner's
   * defaultRuleResult is already correct (And derived filters and anything else).
   *
   * @param groupFilter the expression lifted out of the trigger into the group
   * @param original    the trigger expression before removeTopLevels rewrote it
   */
  def filterFalseResultFor(groupFilter: Expression, original: Expression): Option[Expression] =
    original match {
      // a nullable filter answers Failed for null, the else branch cannot express that
      case IfRelevantExpr(filter, _) if !filter.nullable && coversCondition(groupFilter, filter) =>
        Some(IgnoredRuleExpr)
      case If(predicate, _, falseValue) if coversCondition(groupFilter, predicate) =>
        Some(falseValue)
      case _ => None
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