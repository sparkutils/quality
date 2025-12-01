package com.sparkutils.quality.impl.extension

import com.sparkutils.shim.AbstractInjectableParser
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.ParserInterface

case class ConnectCommandParsers(sparkSession: SparkSession, delegate: ParserInterface) extends AbstractInjectableParser(sparkSession, delegate) with Logging {

}
