package com.sparkutils.quality.impl

import org.yaml.snakeyaml.{LoaderOptions, Yaml}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.inspector.TagInspector

/**
 * 2.0 (14.0dbr) needs security checks for allowed types.  Forcing it on 3.5 so we can debug
 */
object YamlDecoder {

  // being explicit for cve-2022-1471 protection
  val allowed = Set(
    "java.math.BigDecimal",
    "java.lang.String",
    "java.lang.Long",
    "java.lang.Short",
    "java.lang.Integer",
    "java.lang.Float",
    "java.lang.Double",
    "java.lang.String",
    "java.lang.Boolean",
    "java.lang.Byte",
  )

  def yaml: Yaml = {
    val tagInspector = new TagInspector {
      override def isGlobalTagAllowed(tag: Tag): Boolean =
        allowed.contains(tag.getClassName)
    }
    val lo = new LoaderOptions()
    lo.setTagInspector(tagInspector)
    val generator = new Yaml(new Constructor(lo))
    generator
  }
}
