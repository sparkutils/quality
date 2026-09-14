package com.sparkutils.quality

import com.sparkutils.quality.impl.{EncodersImplicits, IdEncodersImplicits, IntEncodersImplicits}

/**
 * Imports the common implicits
 */
package object implicits extends IntEncodersImplicits with IdEncodersImplicits
  with EncodersImplicits {

}
