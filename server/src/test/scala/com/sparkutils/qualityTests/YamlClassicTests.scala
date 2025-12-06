package com.sparkutils.qualityTests

import com.sparkutils.qualityTests.YamlTests.UseFullScalarType
import com.sparkutils.qualityTests.util.{ClassicSharedTests, SharedConnectTests}

class YamlClassicTests extends SharedConnectTests {

  import com.sparkutils.quality.impl.YamlDecoder

  test("decimalViaYaml") {
    evalCodeGens {
      val s = sparkSession
      import s.implicits._
      val str =
        sparkSession.sql(s"select to_yaml(cast(1234.50404 as decimal(30,10)), $UseFullScalarType) r").as[String].head()

      val yaml = YamlDecoder.yaml

      val dec = BigDecimal(1234.50404).setScale(10).bigDecimal
      val obj = yaml.load[java.math.BigDecimal](str);
      assert(obj == dec)
    }
  }

}
