package com.sparkutils.manual

import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeFormatter, CodeGenerator}
import org.scalameter.api.{Bench, _}
import com.sparkutils.quality.impl.util.LazySubExprCodeGen.{isNullArray, initialisationArray}

case class Expr(init: String, rowPrep: String, name: String, funDefinition: String)

case class LazyCSEExpr(eval: Expr, isNull: Expr)

case class CodeInput(initDefs: String, init: String, lazyCSEs: Seq[LazyCSEExpr])

/**
 * Simulates four cases for n exprs:
 *
 * 1) using arrays for initialised and null (n + 3 variables) :
 *
 * 1a) new array each time (2 allocations per row, n + 2 variables)
 *
 * 1b) memcpy over existing arrays (3 allocations per partition)
 *
 * 1c) clone from default (1 per partition, 2 per row)
 *
 * 2) variables for all (3n variables)
 */
object LazyCSEApproachBenchmark extends Bench.OfflineReport {
/*
  val min  = 250
  val max  = 500
  val step = 250
*/
  val min  = 250
  val max  = 1500
  val step = 250


  def code(codeInput: CodeInput): Int => Int = {
    import codeInput._

    val funGroups =
      lazyCSEs.map(c =>
        s"""
          if (!${c.isNull.name}) {
            i += ${c.eval.name};
          }
        """
      ).grouped(100).zipWithIndex.map{ case (g, i) =>
        s"""

        public int runRow$i(int in) {
          int i = in;

          ${g.mkString("\n")}

          return i;
        }
        """
      }.toSeq


    val funCalls =
      for { i <- 0 until funGroups.length } yield s" i += runRow$i(i);"

    val body =
      s"""
      public scala.runtime.AbstractFunction1<Integer, Integer> generate(Object[] references) {
        return new Test();
      }

      final class Test extends scala.runtime.AbstractFunction1<Integer, Integer> {

        $initDefs

        // decls
        ${lazyCSEs.map(c => s"""
          ${c.eval.init}
          ${c.isNull.init}
          """ ).mkString("\n")}

        // functions
        ${lazyCSEs.map(c => s"""
          ${c.eval.funDefinition}
          ${c.isNull.funDefinition}
          """ ).mkString("\n")}

        // call groups
        ${funGroups.mkString("\n")}

        public Test() {
          $init
        }

        public int runRow(int in) {
          int i = in;

          ${funCalls.mkString("\n")}

          return i;
        }

        public Object apply(Object in) {
          ${
            lazyCSEs.map(c => s"""
              ${c.eval.rowPrep}
              ${c.isNull.rowPrep}
              """ ).mkString("\n")
          }
          return (Integer)runRow((int)((Integer)in));
        }

      }
      """


    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(body, Map.empty))

    //println(body)
    val f = CodeGenerator.compile(code)._1.generate(Array.empty).asInstanceOf[Integer => Integer]
    (i: Int) => f(i)
  }

  val generator = Gen.range("variableCount")(min, max, step)

  def evaluate[T](f: Int => CodeInput)(param: Int) = {
    val codeInput = f((max - param) / step)
    val inst = code(codeInput)
    for{
      i <- 0 until 10000000
    } {
      inst(i)
    }
  }

  def isInitialised(i: Int) = s"$initialisationArray[$i]"
  def setInitialised(i: Int) = s"$initialisationArray[$i] = true;"

  def isNull(i: Int) = s"$isNullArray[$i]"

  def arrays(init: Int => String)(count: Int): CodeInput =
    CodeInput(
      s"""
        private boolean[] $initialisationArray = new boolean[$count];
        private boolean[] $isNullArray = new boolean[$count];

        private boolean[] ${initialisationArray}_default = new boolean[$count];
        """,
        init(count),
      for {
        i <- 0 until count
      } yield {
        val funPost = "Fun"
        LazyCSEExpr(
          Expr(s"private int subExpr$i;", "", s"((${isInitialised(i)}) ? subExpr$i : eval$funPost$i() )",
          s"""
             |private int eval$funPost$i() {
             |  //if (!${isInitialised(i)}) {
             |    subExpr$i = $i;
             |    ${isNull(i)} = false;
             |    ${setInitialised(i)}
             |  //}
             |  return subExpr$i;
             |}
                 """.stripMargin
          ),
          Expr(s"", "", s"((${isInitialised(i)}) ? ${isNull(i)} : isNull$funPost$i() )",
            s"""
               |private boolean isNull$funPost$i() {
               |  //if (!${isInitialised(i)}) {
               |    eval$funPost$i();
               |  //}
               |  return ${isNull(i)};
               |}
                 """.stripMargin
          )
        )
      }
    )

  def arraysCall(init: Int => String)(count: Int): CodeInput =
    CodeInput(
      s"""
        private boolean[] $initialisationArray = new boolean[$count];
        private boolean[] $isNullArray = new boolean[$count];

        private boolean[] ${initialisationArray}_default = new boolean[$count];
        """,
      init(count),
      for {
        i <- 0 until count
      } yield {
        val funPost = "Fun"
        LazyCSEExpr(
          Expr(s"private int subExpr$i;", "", s"eval$funPost$i()",
            s"""
               |private int eval$funPost$i() {
               |  if (!${isInitialised(i)}) {
               |    subExpr$i = $i;
               |    ${isNull(i)} = false;
               |    ${setInitialised(i)}
               |  }
               |  return subExpr$i;
               |}
                 """.stripMargin
          ),
          Expr(s"", "", s"isNull$funPost$i()",
            s"""
               |private boolean isNull$funPost$i() {
               |  if (!${isInitialised(i)}) {
               |    eval$funPost$i();
               |  }
               |  return ${isNull(i)};
               |}
                 """.stripMargin
          )
        )
      }
    )

  def varOf(count: Int): CodeInput =
    CodeInput(
      s"""
        """,
      "",
      for {
        i <- 0 until count
      } yield {
        val funPost = "Fun"
        LazyCSEExpr(
          Expr(s"""
              private int subExpr$i;
              private boolean isInitialised$i;
            """, s"isInitialised$i = false;", s"( (isInitialised$i) ? subExpr$i : eval$funPost$i() )",
            s"""
               |private int eval$funPost$i() {
               |  //if (!isInitialised$i) {
               |    subExpr$i = $i;
               |    isNull$i = false;
               |    isInitialised$i = true;
               |  //}
               |  return subExpr$i;
               |}
                 """.stripMargin
          ),
          Expr(s"private boolean isNull$i;", "", s"( (isInitialised$i) ? isNull$i : isNull$funPost$i() )",
            s"""
               |private boolean isNull$funPost$i() {
               |  //if (!isInitialised$i) {
               |    eval$funPost$i();
               |  //}
               |  return isNull$i;
               |}
                 """.stripMargin
          )
        )
      }
    )

  def fill(f: Int => CodeInput): Seq[CodeInput] =
    for{ i <- min to max by step} yield {
      f(i)
    }

  // pre-build so they aren't measured
  /*val newArrays = fill(
      arrays(size =>
        s"""
            $initialisationArray = new boolean[$size];
            $isNullArray = new boolean[$size];
             """
    )
  )
*/
  val memCpy = fill(arrays(
      size =>
        s"""
            System.arraycopy(${initialisationArray}_default, 0, $initialisationArray, 0, $size);
            System.arraycopy(${initialisationArray}_default, 0, $isNullArray, 0, $size);
             """
    ))

  val memCpyCall = fill(arraysCall(
    size =>
      s"""
            System.arraycopy(${initialisationArray}_default, 0, $initialisationArray, 0, $size);
            System.arraycopy(${initialisationArray}_default, 0, $isNullArray, 0, $size);
             """
  ))
  /*

    val clones = fill(arrays(
        _ =>
          s"""
              $initialisationArray = (boolean[]) ${initialisationArray}_default.clone();
              $isNullArray = (boolean[]) ${initialisationArray}_default.clone();
               """
      ))

    val vars = fill(varOf)

    performance of "full blown vars" config (
      exec.minWarmupRuns -> 2,
      exec.maxWarmupRuns -> 4,
      exec.benchRuns -> 4,
      exec.jvmcmd -> (System.getProperty("java.home")+"/bin/java"),
      exec.jvmflags -> Args.args
      //  verbose -> true
    ) in {

      measure method "vars" in {
        using(generator) in evaluate(vars)
      }

    }
  */
  performance of "lazy cse arrays" config (
    exec.minWarmupRuns -> 2,
    exec.maxWarmupRuns -> 4,
    exec.benchRuns -> 4,
    exec.jvmcmd -> (System.getProperty("java.home")+"/bin/java"),
    exec.jvmflags -> Args.args
    //  verbose -> true
  ) in {

    /*measure method "new arrays" in {
      using(generator) in evaluate(newArrays)
    }*/
    measure method "memCpy" in {
      using(generator) in evaluate(memCpy)
    }

    measure method "memCpyCall" in {
      using(generator) in evaluate(memCpyCall)
    }

/*    measure method "clone" in {
      using(generator) in evaluate(clones)
    }
*/
  }

}
