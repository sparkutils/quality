package org.apache.spark.sql

import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem
import com.sparkutils.quality.impl.extension.QualitySparkExtension
import org.apache.hadoop.fs.local.BareStreamingLocalFileSystem
import org.apache.spark.sql.SparkSessionBuilder.CATALOG_IMPL_KEY
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}

object SparkBuilderHelper {

  lazy val scc = classOf[SparkSessionExtensions].getClassLoader.loadClass("org.apache.spark.SparkContext")
  lazy val scoc = classOf[SparkSessionExtensions].getClassLoader.loadClass("org.apache.spark.SparkConf")
  lazy val sccc = scc.getConstructor(classOf[SparkConf])
  lazy val set = scoc.getMethod("set", classOf[String], classOf[String])

  /**
   * spawns a new classic session with a seperate spark context
   * @param withHive
   * @param format
   * @param classicHostMode
   * @return
   */
  def build(withHive: Boolean, format: String, classicHostMode: String): (SparkSession, () => Unit) = {

    val sc = SparkContext.getActive.get
    val se = SparkEnv.get
    SparkContext.clearActiveContext()

    val conf = {
      val c = new SparkConf()
      set.invoke(c, "spark.master", s"local[$classicHostMode]")
      set.invoke(c, "spark.app.name", s"testApp")
      if (withHive) {
        set.invoke(c, CATALOG_IMPL_KEY, "hive")
      }
      if (format == "delta") {
        set.invoke(c, "spark.sql.extensions", classOf[QualitySparkExtension].getName() + ",io.delta.sql.DeltaSparkSessionExtension")
        set.invoke(c, "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      }
      if (System.getProperty("os.name").startsWith("Windows")) {
        set.invoke(c, "spark.hadoop.fs.file.impl", classOf[BareLocalFileSystem].getName)
        set.invoke(c, "spark.hadoop.fs.AbstractFileSystem.file.impl", classOf[BareStreamingLocalFileSystem].getName)
      }
      set.invoke(c, "spark.ui.enabled", "false")
      set.invoke(c, "spark.sql.extensions", classOf[QualitySparkExtension].getName())
      c

    }
    val newSC = sccc.newInstance(conf)
    val tsparkSession = SparkSession.builder().sparkContext(newSC.asInstanceOf[SparkContext]).create()

    SparkSession.setActiveSession(tsparkSession)
    tsparkSession.sparkContext.setLogLevel("ERROR")
    (tsparkSession, () => {
      SparkContext.setActiveContext(sc)
      SparkEnv.set(se)
    })
  }
}
