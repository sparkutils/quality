package com.sparkutils.quality.utils

import com.sparkutils.quality.RuleSuiteGroup
import com.sparkutils.quality.impl.RuleSuiteHelpers
import org.apache.commons.io.IOUtils

import java.io.{FileInputStream, FileOutputStream}

/**
 * usable during test break points
 */
object RuleSuiteGroupIOUtils {

  def toFile(ruleSuiteGroup: RuleSuiteGroup, target: String): Unit = {
    val file = new java.io.File(target)
    if (file.exists()) {
      file.delete()
    }
    val fos = new FileOutputStream(target)
    try {
      fos.write(
        RuleSuiteHelpers.serializeGroup(ruleSuiteGroup)
      )
      fos.flush()
    } finally {
      fos.close()
    }
  }

  def fromFile(source: String): RuleSuiteGroup = {
    val fis = new FileInputStream(source)
    try {
      RuleSuiteHelpers.deserializeGroup( IOUtils.toByteArray( fis ) )
    } finally {
      fis.close()
    }
  }

}
