package com.sparkutils.quality

import com.sparkutils.quality.impl._
import com.sparkutils.quality.impl.util.VariablesLookup
import com.sparkutils.shim.expressions.Names.toName
import org.apache.spark.internal.Logging
import org.apache.spark.sql.ClassicQualitySparkUtils
import org.apache.spark.sql.ShimUtils.arguments
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction}
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Literal}

