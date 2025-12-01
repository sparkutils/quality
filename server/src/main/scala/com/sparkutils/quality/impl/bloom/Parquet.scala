package com.sparkutils.quality.impl.bloom

import java.io.{ByteArrayInputStream, File, ObjectInputStream}
import com.sparkutils.quality.BloomModel
import com.sparkutils.quality.QualityException.qualityException
import com.sparkutils.quality.impl.bloom.parquet.{BlockSplitBloomFilterImpl, Bloom, BloomFilter, BloomHash, BucketedCreator, BucketedFilesRoot, FileRoot}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.shim.expressions.InputTypeChecks
import org.apache.spark.sql.types._

trait ParquetBloomAggregator[T <: Bloom[_]] extends InputTypeChecks {
  val child, expectedSizeE, expectedFPPE: Expression
  val mutableAggBufferOffset, inputAggBufferOffset: Int = 0

  // doesn't work with aggregators?
  override def inputDataTypes: Seq[Seq[DataType]] = Seq(BloomHash.hashTypes,
    Seq(LongType, IntegerType, ShortType), Seq(DoubleType))

  protected lazy val expectedSize: Int =
    com.sparkutils.quality.optimalNumOfBits(
      { val eSize = expectedSizeE.eval()
        eSize match {
          case e: Long => e.toInt
          case e: Int => e
          case e: Short => e.toInt
          case _ => qualityException(s"Should have been Short, Long or Integer but was: $eSize ${eSize.getClass.getSimpleName}")
        }},
      {
        val fpp = expectedFPPE.eval()
        fpp match {
          case f: org.apache.spark.sql.types.Decimal => f.toDouble
          case d: Double => d
          case _ => qualityException(s"Should have been Double or Decimal but was: $fpp ${fpp.getClass.getSimpleName}")
        }
      } / 8 )

  lazy val converter = BloomExpressionLookup.bloomLookupValueConverter(child)

  def nullable: Boolean = false

  def dataType: DataType = BinaryType // closest, worse case we lose the input numBuckets, numHashFunctions

  def children: Seq[Expression] = Seq(child, expectedSizeE, expectedFPPE)

}

trait ParquetBloomFPP {
  val expectedSizeE, expectedFPPE: Expression
  protected lazy val fpp = {
    val ef = expectedFPPE.eval()
    ef match {
      case d: Double => d
      case d: org.apache.spark.sql.types.Decimal => d.toDouble
    }
  }
}

trait TypedParquetBloomAggregator[T <: Bloom[_]] extends
  TypedImperativeAggregate[T] with ParquetBloomAggregator[T] {

  implicit def bloomDes: BloomDeserializer[T]

  override def update(buffer: T, input: InternalRow): T = {
    val r = child.eval(input)
    val ar = converter(r)
    buffer += ar
    buffer
  }

  override def merge(buffer: T, input: T): T= {
    (buffer |= input.asInstanceOf[buffer.BloomType]).asInstanceOf[T]
  }

  override def eval(buffer: T): Any = serialize(buffer)

  override def serialize(buffer: T): Array[Byte] = buffer.serialized

  override def deserialize(storageFormat: Array[Byte]): T = Parquet.deserialize[T](storageFormat)

}

case class ParquetAggregator(child: Expression, expectedSizeE: Expression, expectedFPPE: Expression,
                             override val mutableAggBufferOffset: Int = 0,
                             override val inputAggBufferOffset: Int = 0) extends TypedParquetBloomAggregator[BlockSplitBloomFilterImpl] {
  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
  copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
  copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def createAggregationBuffer(): BlockSplitBloomFilterImpl = new BlockSplitBloomFilterImpl(expectedSize)

  implicit def bloomDes: BloomDeserializer[BlockSplitBloomFilterImpl] = Parquet.blockDes

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(child = newChildren(0), expectedSizeE = newChildren(1), expectedFPPE = newChildren(2))
}

case class BucketedArrayParquetAggregator(child: Expression, expectedSizeE: Expression, expectedFPPE: Expression, id: Expression,
                                     override val mutableAggBufferOffset: Int = 0,
                                     override val inputAggBufferOffset: Int = 0,
                                     bucketedFilesRoot: BucketedFilesRoot = BucketedFilesRoot(FileRoot(com.sparkutils.quality.bloomFileLocation)))
  extends TypedParquetBloomAggregator[ BucketedCreator[BucketedFilesRoot, Array[Array[Byte]]] ]
  with ParquetBloomFPP {

  protected lazy val numBuckets: Int = {
    val longRes = com.sparkutils.quality.optimalNumberOfBuckets(
      expectedSizeE.dataType match {
        case LongType => expectedSizeE.eval().asInstanceOf[Long]
        case IntegerType => expectedSizeE.eval().asInstanceOf[Int]
        case ShortType => expectedSizeE.eval().asInstanceOf[Short]
      },
      fpp)
    longRes.toByte
  }

  protected lazy val root = bucketedFilesRoot.copy(bloomId = id.eval().toString)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  import BucketedCreator.toLocalFiles
  override def createAggregationBuffer(): BucketedCreator[BucketedFilesRoot, Array[Array[Byte]]] = BucketedCreator(expectedSize, numBuckets, fpp, root)

  implicit def bloomDes: BloomDeserializer[BucketedCreator[BucketedFilesRoot, Array[Array[Byte]]]] = Parquet.largeBucketFileDes

  override def eval(buffer: BucketedCreator[BucketedFilesRoot, Array[Array[Byte]]]): Any = serialize(buffer)
  override def dataType: DataType = BinaryType // passes the BroadcastFiles

  override def children: Seq[Expression] = super.children :+ id

  override def inputDataTypes: Seq[Seq[DataType]] = super.inputDataTypes :+ Seq(StringType)
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(child = newChildren(0), expectedSizeE = newChildren(1), expectedFPPE = newChildren(2), id = newChildren(3))
}


trait BloomDeserializer[T] {
  def deserialize(storageFormat: Array[Byte]): T
}

// TODO bring this to top level imports
object Parquet {

  implicit val blockDes = new BloomDeserializer[BlockSplitBloomFilterImpl] {
    override def deserialize(storageFormat: Array[Byte]) = BlockSplitBloomFilterImpl(storageFormat)
  }

  implicit val largeBucketFileDes = new BloomDeserializer[BucketedCreator[BucketedFilesRoot, Array[Array[Byte]]]] {
    override def deserialize(storageFormat: Array[Byte]) = {
      val ios = new ByteArrayInputStream(storageFormat)
      val oos = new ObjectInputStream(ios)
      val bucketedFiles = oos.readObject().asInstanceOf[BloomModel]
      oos.close()
      ios.close()
      val ar = bucketedFiles.read

      val bucket = BucketedCreator(ar, bucketedFiles.fpp,
        BucketedFilesRoot(FileRoot(bucketedFiles.rootDir), new File(bucketedFiles.rootDir).getParentFile.getName))
      bucket
    }
  }

  // nb scary looking type lambda is really just turning a two typed into a one typed by fixing the SerializedType
  def deserialize[T: BloomDeserializer](storageFormat: Array[Byte]): T = {
    implicitly[BloomDeserializer[T]].deserialize(storageFormat)
  }
}
