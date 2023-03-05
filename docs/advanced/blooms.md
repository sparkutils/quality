---
tags: advanced
---

# Bloom Filters

Bloom Filters are probabalistic data structures that, for a given number of items and a false positive probability (FPP) provides a mightContain function.  This function *guarantees* that if an item is not in the bloom filter it will return false, however if it returns true this is to a probability defined by the FPP value.

In contrast to a Set which requires the items (or at least their hash values) to be stored individually blooms make use of multiple blocks and apply bit setting based on hashes of the input value over some function.  These resulting blocks and bitsets are far smaller in memory and storage usage than a typical set.  For example it's possible to store hundreds of millions of items within a bloom and still keep withn a normal Java byte array boundary.

This act of using bit flipping also allows blooms to be or'd for the same size and FPP, which is great for aggregation functions in Spark.

Whilst blooms are great the guarantees break when:

1. The number of items far exceeds the initial size used to create the bloom - false is still guaranteed to not be present but the true value will no longer represent FPP, the bloom has degraded
2. The number of bits required to store the initial number of items at the FPP exceed what can be represented by the bloom algorithm.  

If you attempt to store billions of items within a bloom at a high FPP you will quickly fall foul of 2, and this is easily done with both the Spark stats.package and the current bloom filters on Databricks.  This makes them next to useless for large dataset lookups on _typical_ bloom implementations.

## How does Quality change this?

It can't change the fundemental laws of bloom filters, if you use the number of bits up your bloom filter is next to useless.  You _can_ however add multiple Java byte arrays and bucket the hashes across them.  This works great up to about 1.5b items in a typical aggregation function within Spark, however Spark only allows a maximum of 2Gb for an InternalRow - of which aggregates are stored in.

Quality provides three bloom implementations the Spark stats package, small - which buckets within an InternalRow (1.2-1.5b items max whilst maintaining FPP) - and big which doesn't use Spark aggregations to store the results of aggregations but rather a shared file system such as Databricks dbfs.

Both the small and big bloom functions use Parquet's bloom filter implementation which both significantly faster and has better statistical properties than Sparks/Guavas or Breezes.

## What are Bloom Maps?

Bloom Maps are identifiers to a bloom filter.  The examples below show how to create the key is to use the SparkBloomFilter or bloomFilter functions to provide the value and the FPP is required.

```scala
registerBloomMapAndFunction(bloomFilterMap)
```

Both registers the Bloom Map, the smallBloom and bigBloom aggregation functions and the probabilityIn function.
 
## Using the Spark stats package

```scala
// generate a dataframe with an id column
val df = sqlContext.range(1, 20)
// build a bloomfilter over the id's
val bloom = df.stat.bloomFilter("id", 20, 0.01)
// get the fpp and build the map
val fpp = 1.0 - bloom.expectedFpp()
val bloomFilterMap = SparkSession.active.sparkContext.broadcast( Map("ids" -> (SparkBloomFilter(bloom), fpp)) )

// register the map for this SparkSession
registerBloomMapAndFunction(bloomFilterMap)
// lookup the result of adding column's a and b against that bloom filter for each row
otherSourceDF.withColumn("probabilityInIds", expr("probabilityIn(a + b, 'ids')"))
```

The stats package bloomFilter function has severe limitations on a single field and does not allow expressions but through the SparkBloomFilter lookup function is integrated with Quality anyway.

## Using the Quality bloom filters

The small and big bloom functions take a single expression parameter however it can be built from any number of fields or field types.  Future versions will allow a flexible number of fields to be added to the hash function "see here" #19.

* smallBloom( column, expected number of items, fpp ) - an SQL aggregate function which generates a BloomFilter Array[Byte] for use in probabilityIn or rowId:
```scala
 val aggrow = orig.select(expr(s"smallBloom(uuid, $numRows, 0.01)")).head()
 val thebytes = aggrow.getAs[Bytes](0)
 val bf = bloomLookup(thebytes)
 val fpp = 0.99
 val blooms: BloomFilterMap = Map("ids" -> (bf, fpp))
```
* bigBloom( column, expected number of items, fpp ) - can only be run on large memory sized workers and executors and can cover billions of rows while maintaining the FPP:
```scala
// via the expression
val interim = df.selectExpr(s"bigBloom($bloomOn, $expectedSize, $fpp, '$bloomId')").head.getAs[Array[Byte]](0)
val bloom = com.sparkutils.quality.impl.bloom.parquet.BucketedFiles.deserialize(interim)
bloom.cleanupOthers()
bloom
val blooms: BloomFilterMap = Map("ids" -> (bloomLookup(bloom), fpp))

// via the utility function, defaults to 0.01 fpp
val bloom = bloomFrom(df, "id", expectedsize)
val blooms: BloomFilterMap = Map("ids" -> (bloomLookup(bloom), 1 - bloom.fpp))

```

In testing the bigBloom creation over 1.5b rows on a small 4 node cluster took less than 8m to generate, using a resulting bloom however is far easier to load and distribute and constant time for lookups.  Whilst the actual big bloom itself cannot be directly broadcast only the file location of the resulting bloom is and each node on the cluster directly loads it from the ADLS (or other hopefully fast store for the multiple GBs).

To change the base location for blooms use the sparkSession.sparkContext.setLocalProperty("sparkutils.quality.bloom.root") to specify the location root.

## Expressions which take expression parameters

* probabilityIn( content to lookup, bloomfilterName ) - returns the fpp value of a filter lookup against the bloomFilter with bloomFilterName in the registered BloomFilterMap, which works with the Spark stats package, small and big blooms.

