package com.sparkutils.quality

import frameless.TypedColumn
import org.apache.spark.sql.Row

package object impl { //extends Udf2 {

  /**
    * Easier type to work with for UserDefinedFunction A -> R on a Row
    * @tparam A input type
    * @tparam R return type
    */
  type TypedRowFunction[A, R] = TypedColumn[Row, A] => TypedColumn[Row, R]

}
