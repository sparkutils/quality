package com.sparkutils.quality.impl.imports

import com.sparkutils.quality.QualityException.qualityException
import org.apache.spark.sql.{DataFrame, ShimUtils, SparkSession}

object ResolveUtil {

  protected[quality] def checkResolveMakesSenseOrClassic(resolveWith: Option[DataFrame]): Boolean = {
    if (resolveWith.exists(df => !ShimUtils.isClassic(df.sparkSession))) {
      qualityException("resolveWith is being used with Connect, this is not a valid combination")
    }
    resolveWith.isDefined || ShimUtils.isClassic(SparkSession.active)
  }

}
