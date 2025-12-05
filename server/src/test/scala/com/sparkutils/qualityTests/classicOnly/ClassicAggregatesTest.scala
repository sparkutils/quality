package com.sparkutils.qualityTests.classicOnly

import com.sparkutils.quality.functions.{agg_expr, inc, return_sum}
import com.sparkutils.quality.impl.ReWriteConstants.INC_REWRITE_GENEXP_ERR_MSG
import com.sparkutils.quality.{Id, LambdaFunction, registerLambdaFunctions}
import com.sparkutils.qualityTests.AggregatesTestBase
import com.sparkutils.qualityTests.util.ClassicSharedTests
import com.sparkutils.testing.SparkVersions.sparkVersion
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.DecimalType

class ClassicAggregatesTest extends ClassicSharedTests with AggregatesTestBase {
  test("decimalPrecisionHofTest") {
    funNRewrites {
      val sf = LambdaFunction("myinc", "entry -> entry + dec", Id(0, 3))
      val sf2 = LambdaFunction("myinc", "(entry, f) -> entry + dec + f", Id(0, 3))
      val rf = LambdaFunction("myretsum", "(sum, count) -> sum", Id(0, 3))
      val rf2 = LambdaFunction("myretsum", "(sum, f, count) -> sum + f", Id(0, 3))
      registerLambdaFunctions(Seq(sf, sf2, rf, rf2))
      // NOTE on spark 2.4 it will not auto cast to BigDecimal on part 1 and 3 below
      // as such we wrap sql...
      val (pre, post) =
        if (sparkVersion != "2.4") // not spark 2.4
          ("", "as agg")
        else
          ("cast(", " as DECIMAL(37,12)) as agg")

      // (1) test with wider partial application
      doDecimalPrecisionTest(expr(s"${pre}aggExpr('DECIMAL(37,18)', dec IS NOT NULL, myinc(_()), myretsum(_(), cast(0.0 as DECIMAL(37,18)), _())) $post"))
      // (2) test with 1:1 hof
      doDecimalPrecisionTest(expr("aggExpr('DECIMAL(37,18)', dec IS NOT NULL, myinc(_()), myretsum(_(), _())) as agg"))
      // (3) test with partial on sum
      doDecimalPrecisionTest(expr(s"${pre}aggExpr('DECIMAL(37,18)', dec IS NOT NULL, myinc(_(), cast(0.0 as DECIMAL(37,18))), myretsum(_(), _())) $post"))
    }
  }

  test("decimalPrecisionIncExprTest") {
    funNRewrites {
      doDecimalPrecisionTest(expr("aggExpr('DECIMAL(37,18)', dec IS NOT NULL, inc(dec + 0), returnSum()) as agg"))
    }
  }

  test("decimalPrecisionIncExprDSLTest") {
    funNRewrites {
      doDecimalPrecisionTestF(df => agg_expr(DecimalType(37, 18), df("dec").isNotNull, inc(df("dec") + 0), return_sum) as "agg")
    }
  }

  test("decimalPrecisionNO_REWRITEIncTest") {
    funNRewrites {
      try {
        doDecimalPrecisionTest(expr("aggExpr('NO_REWRITE', dec IS NOT NULL, inc('DECIMAL(37,18)', cast( dec as DECIMAL(37,18))), returnSum('DECIMAL(37,18)')) as agg"))
        fail("Should have thrown " + INC_REWRITE_GENEXP_ERR_MSG)
      } catch {
        case t: Throwable if t.getMessage.contains(INC_REWRITE_GENEXP_ERR_MSG) =>
        // passed
        case t: Throwable =>
          fail("Should have thrown " + INC_REWRITE_GENEXP_ERR_MSG + " but threw ", t)
      }
    }
  }

  test("decimalPrecisionDeprecatedIncTest") {
    funNRewrites {
      doDecimalPrecisionTest(expr("aggExpr(dec IS NOT NULL, inc('DECIMAL(37,18)', dec ), returnSum('DECIMAL(37,18)')) as agg"))
    }
  }

}
