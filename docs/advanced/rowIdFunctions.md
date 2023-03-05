---
tags: advanced
---

Row ID functions are at least 160bit, made of a lower base id and two longs.  There are 4 distinct implementations:

1. Random Number, a 128bit payload based on XO_RO_SHI_RO_128_PP
2. Field Based, 128bit MD5 payload based on fields e.g. for DataVault style approaches
3. Provided, an Opaque ID payload, typically 128bit, provided by some upstream system fields (MD5 is not used under the hood)
4. Guaranteed Unique, 160bit ID based on Twitters snowflake IDs at Spark scale - requires MAC addresses to be stable and unique on a driver  

These IDs use the "base" field to provide extensibility but comparisons must include all three fields (or more longs should they be added).

From a performance perspective you should transform the column to make the structure into top-level fields via
```scala
selectExpr("*","myIDField.*").drop("myIDField")
```

* rngID('prefix') - generates a Random 128bit number with each column name prefixed for easy extraction
* uniqueID('prefix') - generates a unique 160bit ID with each column name prefixed for easy extraction
* fieldBasedID('prefix', 'messagedigest', exp1, exp2, *) - generates a digest based e.g. 'MD5' identifier based on an expression list
* providedID('prefix', longArrayBasedExpression) - generates a providedID based on supplied array of two longs expression
* murmur3ID('prefix', exp1, exp2, *) - generates and ID using hashes based on a version of murmur3 - not cryptographically secure but fast
* idEqual('left_prefix', 'right_prefix') - (SQL only) tests the two top level field IDs by adding the prefixes, note this does allow predicate push-down / pruning etc. (NB further versions may be added when 160bit is exceeded)

!!! note "Id's can be 96-bit or larger multiples of 64"
    The algorithm you chose to use for generating Ids will change the length of underlying longs, idEqual cannot be used on different lengths but you can easily replace this with a lambda of the correct length.

!!! information "There are many different hash impls"
    The fieldBasedID functions have a family of alternatives for MessageDigest, ZA based hashes and Guava based Hashers.  See [SQL Functions](../sqlfunctions.md) and look for the Hash and ID tags.

### fieldBasedID with MD5 - Seems far slower than other approaches

It's definitely slower than either uniqueId or rngID.  If your use case allows it, consider murmur3ID if this is sufficient, it's slightly faster as is the XXH3 za hash.  MD5 was chosen based on the ubiquity of implementations including on backends (e.g. allowing datavault style approaches).

### Guaranteed Unique ID - How?

In order to lock down a globally (within a Spark using routable IP address space) ID you need to make sure a given machine, point in time and partition (thread) is unique.

Your networking / vendor setup should guarantee the machines MAC Address is unique for your Spark Driver, Spark guarantees that the partition id, although re-usable, does not get re-used within a Spark cluster and for a given ms since an epoch we can lock down a range of row numbers.  This leaves the following storage model:

```{.mermaid .quality_gantt}
gantt
    dateFormat YYYY-MM-DD
	axisFormat %j
    title       Bit Layout
	todayMarker off
    
    section First Int
    Unique ID Type and Reserved Space :active, start, 2021-01-01, 8d
    First 3 Bytes of MAC               :  startmac, after start, 24d
    
    section First Long
    Last 3 Bytes of MAC                :endmac, after startmac, 24d
    Spark Partition      :partition, after endmac, 32d
    First 8 bits of Timestamp    :starttimestamp, after partition, 8d
	
	section Second Long
	Rest of Timestamp	:done, endtimestamp, after starttimestamp, 33d
	Row number in Partition :rowid, after endtimestamp, 31d
```

When Spark starts a new partition the uniqueID expression resets the timestamp and partition and each row evaluates the rowid.  When 32bits of rowid would be hit the timestamp is reset and the count resets to 0 allowing over a billion rows per ms.

This approach is faster than rngID but also means rows written to the same partitions have statistically incrementing id's allowing Parquet statistical ranges to be used for all three values in predicate pushdowns.

