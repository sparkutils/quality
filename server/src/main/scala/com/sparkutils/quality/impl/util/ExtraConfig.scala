package com.sparkutils.quality.impl.util

import com.sparkutils.quality.getConfig

import scala.util.Try

object ExtraConfig {

  def getX[T](keyName: String, config: Map[String, String], default: T)(f: String => T): T =
    config.get(keyName).orElse(
      Option(getConfig(keyName, default = null))
    ).map(s => Try{f(s)}.getOrElse(default)).getOrElse(default)

  implicit class ConfigMapOps(val config: Map[String, String]) {
    def boolean(keyName: String, default: Boolean = false): Boolean =
      getX(keyName, config, default)(_.toBoolean)

    def string(keyName: String, default: String = ""): String =
      getX(keyName, config, default)(identity)

    def int(keyName: String, default: Int): Int =
      getX(keyName, config, default)(_.toInt)
  }

}
