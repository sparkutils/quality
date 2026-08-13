package com.sparkutils.quality.impl

import org.yaml.snakeyaml.Yaml

/**
 * 2.0 (14.0dbr) needs security checks for allowed types.  Forcing it on 3.5 so we can debug
 */
object YamlDecoder {
  def yaml: Yaml = {
    val generator = new Yaml()
    generator
  }
}
