package com.sparkutils.manual

import com.sparkutils.quality.impl.RuleLogicUtils.anyToRuleResultInt
import com.sparkutils.quality._
import org.scalameter.api.{Bench, _}

/**
 * Make sure to run StatisticsThroughputBenchmarkTestSetup first to create the files
 */
object ToResultBenchmark extends Bench.OfflineReport {
/*

  // typically just from compilation
  def anyToRuleResultInt(any: Any): Int =
    any match {
      case b: Boolean => if (b) PassedInt else FailedInt
      case 0 | 0.0 | 0L => FailedInt
      case 1 | 1.0 | 1L => PassedInt
      case -1 | -1.0 | -1L | UTF8Str("softfail" | "maybe") => SoftFailedInt
      case -2  | -2.0 | -2L | UTF8Str("disabledrule" | "disabled") => DisabledRuleInt
      case -3  | -3.0 | -3L | UTF8Str("ignoredrule" | "ignored") => IgnoredRuleInt
      case -4  | -4.0 | -4L => DefaultRuleInt
      case -5  | -5.0 | -5L => UnevaluatedRuleInt
      case d: Double => (d * PassedInt).toInt
      case d: Float => (d * PassedInt).toInt
      case d: Decimal => (d.toDouble * PassedInt).toInt
      case UTF8Str("true" | "passed" | "pass" | "yes" | "1" | "1.0") => PassedInt
      case UTF8Str("false" | "failed" | "fail" | "no" | "0" | "0.0") => FailedInt
      case _ => FailedInt // anything else is a fail
    }
 */
  val min  = 1000000
  val max  = 10000000
  val step = 1000000

  val intSet = Seq(SoftFailedInt, DisabledRuleInt, IgnoredRuleInt, DefaultRuleInt, UnevaluatedRuleInt,
    PassedInt, FailedInt, 132534254, 234243242, 33453453)

  val allInts = Seq.fill((max / intSet.size) + intSet.size)(intSet).flatten
  val ints =
    (min to max by step).map( i => i -> allInts.take(i)).toMap

  val generator = Gen.range("rowCount")(min, max, step)

  def evaluate[T](source: Int => Seq[T])(f: Seq[T] => Seq[Int])(param: Int) = {
    f(source(param))
  }

  def directInts(res: Int): Int = {
    if (res >= UnevaluatedRuleInt && res <= PassedInt)
      res
    else
      FailedInt
  }

  val allBooleans = Seq.fill((max / 2) + 2)(Seq(true, false)).flatten
  val booleans =
    (min to max by step).map( i => i -> allBooleans.take(i)).toMap

  def directBooleans(res: Boolean): Int =
    if (res)
      PassedInt
    else
      FailedInt

  performance of "processing ints" config (
    exec.minWarmupRuns -> 2,
    exec.maxWarmupRuns -> 4,
    exec.benchRuns -> 4,
    exec.jvmcmd -> (System.getProperty("java.home")+"/bin/java"),
    exec.jvmflags -> Args.args
    //  verbose -> true
  ) in {

    measure method "any to" in {
      using(generator) in evaluate(ints)(_.map(anyToRuleResultInt))
    }

    measure method "direct" in {
      using(generator) in evaluate(ints)(_.map(directInts))
    }

  }

  performance of "processing booleans" config (
    exec.minWarmupRuns -> 2,
    exec.maxWarmupRuns -> 4,
    exec.benchRuns -> 4,
    exec.jvmcmd -> (System.getProperty("java.home")+"/bin/java"),
    exec.jvmflags -> Args.args
    //  verbose -> true
  ) in {

    measure method "any to" in {
      using(generator) in evaluate(booleans)(_.map(anyToRuleResultInt))
    }

    measure method "direct" in {
      using(generator) in evaluate(booleans)(_.map(directBooleans))
    }

  }

}
