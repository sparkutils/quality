package com.sparkutils.quality

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File, FileInputStream, ObjectInputStream, ObjectOutputStream, Serializable}
import java.nio.{ByteOrder, IntBuffer}
import java.nio.channels.FileChannel

import org.apache.spark.util.sketch.BloomFilter

/**
 * Simple does it contain function to test a bloom
 */
trait BloomLookup {
  def apply(any: Any): Boolean = mightContain(any)

  def mightContain(any: Any): Boolean
}

case class SparkBloomFilter( bloom: BloomFilter ) extends BloomLookup {
  override def mightContain(any: Any): Boolean =
    any match {
      case s: String => bloom.mightContainString(s)
      case b: Array[Byte] => bloom.mightContainBinary(b)
      case l: Long => bloom.mightContainLong(l)
      case _ => bloom.mightContain(any)
    }
}

/**
 * Represents the shared file location of a bucked bloom filter.  There should be files with names 0..numBuckets containing
 * the same number of bytes representing each bucket.
 *
 * @param rootDir  The directory which contains each bucket
 * @param fpp  The fpp for this bloom - note it is informational only and will not be used in further processing
 * @param numBuckets The number of buckets within this bloom
 */
case class BloomModel(rootDir: String, fpp: Double, numBuckets: Int) extends Serializable {
  /**
   * Provides memory mapped buffers from the underlying files
   * @return
   */
  def maps: Seq[IntBuffer] =
    (0 until numBuckets).map { i =>
      val file = new File(rootDir, i.toString)
      val mapped = new FileInputStream(file).getChannel.map(FileChannel.MapMode.READ_ONLY, 0, file.length())
      mapped.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer()
    }

  /**
   * Serializes the definition of this bucketfiles, not the underlying bytes of the bloom
   * @return
   */
  def serialize: Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(this)
    oos.flush()
    val bytes = bos.toByteArray
    bos.close()
    bytes
  }

  /**
   * Only performed lazily
   * @return
   */
  def read: Array[Array[Byte]] = {
    val size = new File(rootDir, "0").length().toInt // safe as it's never more than an array
    val ar = Array.ofDim[Array[Byte]](numBuckets)
    for { i <- 0 until numBuckets} {
      val f = new File(rootDir, i.toString)
      val fis = new FileInputStream(f)
      ar(i) = Array.ofDim[Byte](size)
      fis.read(ar(i))
      fis.close()
    }
    ar
  }

  /**
   * Removes other directories from the parent root id
   */
  def cleanupOthers(): Unit ={
    val file = new File(rootDir)
    val name = file.getName
    val siblings = file.getParentFile.listFiles().filterNot(_.getName == name)
    siblings.foreach(_.listFiles().foreach(_.delete()))
    siblings.foreach(_.delete())
  }

  /**
   * Removes this bloom's files, this is advised only after you have processed or otherwise saved it's results.  It will remove everything under this bloomid
   */
  def removeThisBloom(): Unit = {
    val file = new File(rootDir)
    val siblings = file.getParentFile.listFiles()
    siblings.foreach(_.listFiles().foreach(_.delete()))
    siblings.foreach(_.delete())
  }
}

object BloomModel {
  /**
   * Deserializes from the bytes, must have been created by a compatible BucketFiles.serialize
   * @param storageFormat
   * @return
   */
  def deserialize(storageFormat: Array[Byte]): BloomModel = {
    val ios = new ByteArrayInputStream(storageFormat)
    val oos = new ObjectInputStream(ios)
    val bucketedFiles = oos.readObject().asInstanceOf[BloomModel]
    oos.close()
    ios.close()
    bucketedFiles
  }
}
