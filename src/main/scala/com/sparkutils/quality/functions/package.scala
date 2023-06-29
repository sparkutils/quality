package com.sparkutils.quality

import com.sparkutils.quality.impl.bloom.BloomFilterLookupFunctionImport
import com.sparkutils.quality.impl.id.{GenericLongBasedImports, GuaranteedUniqueIDImports}
import com.sparkutils.quality.impl.imports.{LongPairImports, PackIdImports, RuleResultImport, RuleRunnerFunctionImports, StripResultTypesFunction}
import com.sparkutils.quality.impl.rng.RngFunctionImports
import com.sparkutils.quality.impl.util.ComparableMapsImports

/**
 * Collection of the Quality Spark Expressions for use in select( Column * )
 */
package object functions extends ComparableMapsImports with GuaranteedUniqueIDImports with GenericLongBasedImports
  with BloomFilterLookupFunctionImport with StripResultTypesFunction with RuleResultImport with PackIdImports
  with RuleRunnerFunctionImports with RngFunctionImports with LongPairImports {

}
