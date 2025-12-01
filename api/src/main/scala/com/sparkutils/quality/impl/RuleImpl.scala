package com.sparkutils.quality.impl

import com.sparkutils.quality.RuleSuite
import com.sparkutils.quality.impl.util.Serializing.toSeq
import org.apache.spark.sql.SparkSession

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream, ObjectStreamClass}


object RuleSuiteHelpers {
  def getSparkClassLoader: ClassLoader = classOf[SparkSession].getClassLoader

  def getContextOrSparkClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)

  protected[quality] def deserialize(in: Array[Byte]): RuleSuite = {
    val os = new ObjectInputStream(new ByteArrayInputStream(in)) {
      override def resolveClass(desc: ObjectStreamClass): Class[_] =
        Class.forName(desc.getName, false, getContextOrSparkClassLoader)
    }
    val suite = os.readObject()
    os.close()
    suite.asInstanceOf[RuleSuite]
  }

  protected[quality] def serialize(ruleSuite: RuleSuite): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val os = new ObjectOutputStream(bos)
    // get rid of List's Vectors are serializable
    os.writeObject(toSeq(ruleSuite))
    val res = bos.toByteArray
    os.close()
    res
  }
}