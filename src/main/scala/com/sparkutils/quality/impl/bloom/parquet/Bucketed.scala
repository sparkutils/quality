package com.sparkutils.quality.impl.bloom.parquet

import com.sparkutils.quality.impl.util.{BytePackingUtils, TSLocal, TransientHolder}
import java.io._
import com.sparkutils.quality.{BloomLookup, BloomModel}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Bucketed {

  // just two is:
  //generated bloom map of 1000000000 entries in 9 m
  //before actual count 0 824404383 (824404383) false positives took 9 m

  //generated bloom map of 1000000000 entries in 9 m
  //before actual count 0 273489593 (273489593) false positives took 11 m <-- saturated even with 4, so need some math to figure out buckets to use

  // generated bloom map of 1000000000 entries in 10 m
  //before actual count 0     24650171 (24650171) false positives took 9 m <-- 8 buckets max on each at 0.001 configured but then only 0.024 real with 8 buckets - still not "bad"

  // generated bloom map of 1000000000 entries in 11 m
  //before actual count 0      1270757 (1270757) false positives took 12 m <-- 15 0.0012

  def whichBloom(hash: Long, buckets: Int): Int = {
    (hash % buckets).abs.toInt  // the below is either 2, 4, or 8 to be useful, 16 is already too many buckets for a byte array
  }


}

case class FileRoot(location: String) {
  def asFile = new File(location)
}

case class BucketedFilesRoot(bloomFileLocation: FileRoot, bloomId: String = java.util.UUID.randomUUID().toString) {
  def fileLocation: File = {
    val root = new File(bloomFileLocation.asFile, bloomId)
    root.mkdir()
    root
  }
}

trait ToSerializedType[T, H] extends Serializable {
  def serializeBuckets(filters: Seq[BlockSplitBloomFilterImpl], fpp: Double, numBuckets: Int, hint: H): Array[Byte]
}

object BucketedCreator {

  /**
   * Generates a bloom filter using the expression passed via bloomOn with optional repartitioning and a default fpp of 0.01.
   * A unique run will be created so other results can co-exist but this can be overridden by specifying bloomId.
   * The other interim result files will be removed and the resulting BucketedFiles can be used in lookups or stored elsewhere.
   *
   * @param dataFrame
   * @param bloomOn a compatible expression to generate the bloom hashes against
   * @param expectedSize
   * @param fpp
   * @param bloomId
   * @param partitions
   * @return
   */
  def bloomFrom(dataFrame: DataFrame, bloomOn: String, expectedSize: Long,
                fpp: Double = 0.01, bloomId: String = java.util.UUID.randomUUID().toString, partitions: Int = 0): BloomModel = {
    val df =
      if (partitions != 0)
        dataFrame.repartition(partitions)
      else
        dataFrame

    val interim = df.selectExpr(s"bigBloom($bloomOn, $expectedSize, cast($fpp as double), '$bloomId')").head.getAs[Array[Byte]](0)
    val bloom = BloomModel.deserialize(interim)
    bloom.cleanupOthers()
    bloom
  }

  val bloomFileLocation = {
    val defaultRoot = SQLConf.get.getConfString("sparkutils.quality.bloom.root", "/dbfs/")

    // if it's running on databricks use a dbfs dir
    val dbfs = new File(defaultRoot)
    val root =
      if (dbfs.exists())
        dbfs
      else {
        val temp = File.createTempFile("quality", "bloom")
        val tempPath = temp.getAbsolutePath
        temp.delete()
        new File(tempPath)
      }

    val fl = new File(root, "quality_bloom")
    fl.mkdirs()
    fl.getAbsolutePath
  }

  implicit val toLocalFiles = new ToSerializedType[Array[Array[Byte]], BucketedFilesRoot] {
    def serializeBuckets(filters: Seq[BlockSplitBloomFilterImpl], fpp: Double, numBuckets: Int, hint: BucketedFilesRoot): Array[Byte] = {
      // verify there is no file, get id until it's done
      var id: String = null
      var dir: File = null
      do {
        id = java.util.UUID.randomUUID().toString
        dir = new File(hint.fileLocation, id)
      } while (dir.exists())
      dir.mkdirs()

      for {i <- 0 until numBuckets} {
        val fos = new FileOutputStream(new File(dir, i.toString))
        filters(i).intBufferGen.get().bytes.fold(
          for {j <- 0 until filters(i).intBuffer.limit} {
            val theInt = filters(i).intBuffer.get(j)
            fos.write(theInt.toByte)
            fos.write((theInt >> 8).toByte)
            fos.write((theInt >> 16).toByte)
            fos.write((theInt >> 24).toByte)
          }
        ) {
          bytes => // faster if we have the underlying
            fos.write(bytes)
        }
        fos.close()
      }

      val bucketedFiles = BloomModel(dir.getAbsolutePath, fpp, numBuckets)
      bucketedFiles.serialize
    }
  }

  /**
   * Creates Buckets of Blooms based on assuming rounding and overflow behaviour for the Array[Byte] produced, leaving a maximum number of buckets of 15.
   *
   * To give an idea of what this means in practice:
   * {{{
   * 2750000000L for 0.1 will yield the max 15 buckets, anything below will be 0.1 (2.7b and below)
   * 1663500000L for 0.01 will yield the max 15 buckets, anything below will be 0.01 (1.6b and below)
   * 1102000000L for 0.001 will yield the max 15 buckets
   *  760000000L for 0.0001 will yield the max 15 buckets
   * }}}
   *
   * NOTE In the current impl overflows and weird results will occur with higher numbers of elements than 2.7b and 0.1.
   *
   * @param numBytes - calculate with optimalNumOfBits
   * @param numBuckets - calculate with optimalNumberOfBuckets, only 15 max are possible with SerializedType as Array[Byte]
   * @return
   */
  def apply[H, SerializedType: ({ type T[SerializedType] = ToSerializedType[SerializedType, H]})#T](numBytes: Int, numBuckets: Int, fpp: Double, hashImpl: BloomHash, hint: H): BucketedCreator[H, SerializedType] =
    BucketedCreator(numBytes, BlockSplitBloomFilterImpl.LOWER_BOUND_BYTES, BlockSplitBloomFilterImpl.UPPER_BOUND_BYTES,
      hashImpl, fpp, numBuckets, implicitly[ToSerializedType[SerializedType, H]], hint)()

  def apply[H, SerializedType: ({ type T[SerializedType] = ToSerializedType[SerializedType, H]})#T](numBytes: Int, numBuckets: Int, fpp: Double, hint: H): BucketedCreator[H, SerializedType] =
    apply(numBytes, numBuckets, fpp, new BloomHashImpl(BloomFilter.XXH64), hint)

  def impl[H, SerializedType: ({ type T[SerializedType] = ToSerializedType[SerializedType, H]})#T](bitArrays: Array[Array[Byte]], hashImpl: BloomHash, fpp: Double, hint: H): BucketedCreator[H, SerializedType] =
    BucketedCreator[H, SerializedType](0, BlockSplitBloomFilterImpl.LOWER_BOUND_BYTES, BlockSplitBloomFilterImpl.UPPER_BOUND_BYTES,
      hashImpl, fpp, bitArrays.length, implicitly[ToSerializedType[SerializedType, H]], hint)( {
      (0 until bitArrays.length).map { i =>
        BlockSplitBloomFilterImpl.fromBytes(bitArrays(i))
      }
    })

  def apply(bitArrays: Array[Array[Byte]], hashImpl: BloomHash, fpp: Double, hint: BucketedFilesRoot): BucketedCreator[BucketedFilesRoot, Array[Array[Byte]]] =
    impl(bitArrays, hashImpl, fpp, hint)

  def apply(bitArrays: Array[Array[Byte]], fpp: Double, hint: BucketedFilesRoot): BucketedCreator[BucketedFilesRoot, Array[Array[Byte]]] =
    apply(bitArrays, new BloomHashImpl(BloomFilter.XXH64), fpp, hint)

}

/**
 * Buckets smaller optimised blooms by a mod on bucket size with array backing by default
 *
 * @param numBytes
 * @param iMinimumBytes
 * @param iMaximumBytes
 * @param hashImpl
 * @param numBuckets max is 15 algo wise but 8 breaks defaults for  spark.driver.maxResultSize on local 1gb it's 4g on db
 */
case class BucketedCreator[H, SerializedType](numBytes: Int, iMinimumBytes: Int, iMaximumBytes: Int, hashImpl: BloomHash, fpp: Double, numBuckets: Int,
                                           toType: ToSerializedType[SerializedType, H], hint: H)(
  val filters: Seq[BlockSplitBloomFilterImpl] = (1 to numBuckets).map(_ => BlockSplitBloomFilterImpl(numBytes, iMinimumBytes, iMaximumBytes, hashImpl)),
  val bucketedFiles: Option[BloomModel] = None
) extends Bloom[SerializedType] {
  type BloomType = BucketedCreator[H, SerializedType]

  /** CTw added the or'ing */
  def |= (that: BucketedCreator[H, SerializedType]): BucketedCreator[H, SerializedType] = {
    checkCompatibility(that)
    // Breeze copies but we'll do in place
    for(i <- 0 until numBuckets) {
      filters(i) |= that.filters(i)
    }
    this
  }

  private def checkCompatibility(that: BucketedCreator[H, SerializedType]): Unit =
    require(iMaximumBytes == that.iMaximumBytes
      && iMinimumBytes == that.iMinimumBytes &&
      hashImpl.hashStrategy == that.hashImpl.hashStrategy
      && filters.zip( that.filters ).forall( p => p._1.intBuffer.limit() == p._2.intBuffer.limit() )
      , "Must have the hash and bitset length to intersect")

  def += (value: Any) = {
    val h = hashImpl.hash(value)
    filters(Bucketed.whichBloom(h, numBuckets)).insertHash(h)
    this.asInstanceOf[BloomType]
  }

  /**
   * Combines all the buckets together via the serialize type.  If this is already linked to memory mapped files
   * do not re-write, just serialize the bucketedFiles reference
   * @return combined serialized blooms
   */
  override def serialized: Array[Byte] = bucketedFiles.fold(toType.serializeBuckets(filters, fpp, numBuckets, hint))(_.serialize)
}

case class ThreadBucketedLookup(arrays: Array[Array[Byte]], hashImpl: BloomHash) extends com.sparkutils.quality.BloomLookup {

  val filters = TransientHolder{ () =>
    arrays.map( ThreadLookup(_, hashImpl) )
  }

  override def mightContain(value: Any): Boolean = {
    val h = hashImpl.hash(value)
    filters.get.apply(Bucketed.whichBloom(h, arrays.length)).findHash(h)
  }
}

/**
 * The default BlockSplitBloomFilter is not threadsafe for lookup.
 * This implementation uses a thread local hash preparation for lookup
 *
 * @param bucketedFiles the bloom
 */
case class ThreadSafeBucketedBloomLookupLazy(bucketedFiles: BloomModel,
                                             hashStrategy: BloomFilter.HashStrategy = BloomFilter.XXH64) extends com.sparkutils.quality.BloomLookup {

  // don't ship the bytes but lazy load on each box
  val arrays = TransientHolder{ () =>
    bucketedFiles.read
  }

  private val impl: TSLocal[ThreadBucketedLookup] = TSLocal[ThreadBucketedLookup]{ () =>

    ThreadBucketedLookup(arrays.get(), new BloomHashImpl(hashStrategy))
  }

  override def mightContain(value: Any): Boolean = impl.get().mightContain(value)

}

/**
 * By default eager, optionally lazy which will read the files from the executor
 */
object ThreadSafeBucketedBloomLookup {
  def lazyLookup(bucketedFiles: BloomModel) = ThreadSafeBucketedBloomLookupLazy(bucketedFiles)
  def apply(bucketedFiles: BloomModel) = {
    val arrays = bucketedFiles.read

    ThreadSafeBucketedBloomLookupEager(arrays)
  }
  def mappedLookup(bucketedFiles: BloomModel) = ThreadSafeBucketedBloomLookupMapped(bucketedFiles)
}

/**
 * The default BlockSplitBloomFilter is not threadsafe for lookup.
 * This implementation uses a thread local hash preparation for lookup
 *
 * @param arrays the bloom
 */
case class ThreadSafeBucketedBloomLookupEager(arrays: Array[Array[Byte]],
                                         hashStrategy: BloomFilter.HashStrategy = BloomFilter.XXH64) extends BloomLookup {

  private val impl: TSLocal[ThreadBucketedLookup] = TSLocal[ThreadBucketedLookup]{ () =>
    ThreadBucketedLookup(arrays, new BloomHashImpl(hashStrategy))
  }

  override def mightContain(value: Any): Boolean = impl.get().mightContain(value)

}

case class ThreadBucketedMappedLookup(bucketedFiles: BloomModel, hashImpl: BloomHash) extends BloomLookup {

  val filters = TransientHolder{ () =>
    bucketedFiles.maps.map( ThreadBufferLookup(_, hashImpl) )
  }

  override def mightContain(value: Any): Boolean = {
    val h = hashImpl.hash(value)
    filters.get.apply(Bucketed.whichBloom(h, bucketedFiles.numBuckets)).findHash(h)
  }
}

/**
 * The default BlockSplitBloomFilter is not threadsafe for lookup.
 * This implementation uses a thread local hash preparation for lookup
 *
 * @param bucketedFiles the bloom
 */
case class ThreadSafeBucketedBloomLookupMapped(bucketedFiles: BloomModel,
                                               hashStrategy: BloomFilter.HashStrategy = BloomFilter.XXH64) extends BloomLookup {

  private val impl: TSLocal[ThreadBucketedMappedLookup] = TSLocal[ThreadBucketedMappedLookup]{ () =>

    ThreadBucketedMappedLookup(bucketedFiles, new BloomHashImpl(hashStrategy))
  }

  override def mightContain(value: Any): Boolean = impl.get().mightContain(value)

}
