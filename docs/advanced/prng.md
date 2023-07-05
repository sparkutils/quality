---
tags: advanced
---

The existing Spark rand function has a few of limitations:

* It generates doubles
* Has a fixed implementation
* Only provides reseeding on each new parition ignoring splittable / jumpable algorithms

The Quality psuedorandom generators produce either 128bit values (two longs) or a configurable number of bytes and, as a result, do not suffer precision issues, they also leverage [RandomSource](https://commons.apache.org/proper/commons-rng/commons-rng-simple/apidocs/org/apache/commons/rng/simple/RandomSource.html) implementations allowing users to choose the algorithm used.

In addition, by leveraging .isJumpable and the resulting [jump function](https://commons.apache.org/proper/commons-rng/commons-rng-client-api/apidocs/org/apache/commons/rng/JumpableUniformRandomProvider.html) the Quality prng function can benefit from the implementations own approach to managing overalapping intervals across the cluster.


## RNG Expressions

* rng_bytes( [number of bytes to fill - defaults to 16], [RandomSource RNG Impl - defaults to 'XO_RO_SHI_RO_128_PP'], [seed - defaults to 0] ) - Uses commons rng to create byte arrays, implementations can be plugged in, when seed is 0 the RNG's default seed generator is used.  Note when a given RNG `isJumpable` then it will use jumping for each partition where possible both improving speed and statistical results.
* rng( [RandomSource RNG Impl - defaults to 'XO_RO_SHI_RO_128_PP'], [seed - defaults to 0] ) - Uses commons rng to create byte arrays, implementations can be plugged in, when seed is 0 the RNG's default seed generator is used.  Note when a given RNG `isJumpable` then it will use jumping for each partition where possible both improving speed and statistical results.
* rng_uuid( expr ) - processes expr with either byte arrays or two longs into a UUID string, it's counterpart [long_pair_from_uuid]({{ config.site_url }}/sqlfunctions/#long_pair_from_uuid) generates two longs
