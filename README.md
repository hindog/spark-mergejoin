# Scalable Join Support for Spark RDDs

An extension to the [Apache Spark](http://spark.apache.org/docs/latest/sql-programming-guide.html) project that provides support for highly-scalable RDD joins using a [Sort-Merge](https://en.wikipedia.org/wiki/Sort-merge_join) join algorithm.

## Overview

#### The Problem

In the standard distribution, Spark includes RDD [join](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.PairRDDFunctions) operators that must collect **all values across all rows on both sides of the join for any given key** into an in-memory buffer before it can output the joined values for that key.  Because of this, you might experience scalability issues when joining large datasets or datasets that exhibit a [low-cardinality](https://en.wikipedia.org/wiki/Cardinality_\(data_modeling\)) or [skewed](https://en.wikipedia.org/wiki/Skewness) distribution of values. 

#### Is there a work-around for this issue when using the existing Spark join operators?

Well... yes and no.  Outlined below are some of the possible resolutions that one might try to work around the issue:

1. **Increase the partition count.**   Increasing the number of partitions won't help, since the source of the problem is related to processing of a **single key** whose values will end up in the same partition, regardless of partition count.

2. **Increasing the executor memory.** This can solve the problem in some cases where we are able to allocate enough memory for every executor to ensure we can handle the largest set of values for any given key. There are several drawbacks to this approach, though:

	1. **Allocating the required amount of memory might not be possible.**  If the maximum physical memory available to any given executor exceeds the amount we need, then we're out of luck.

	2. **It requires us to know, ahead of time, the maximum amount of memory any given executor will need.**  This usually involves a trial-and-error effort which can be time-consuming, especially when production-sized data is required to replicate the issue.  Also if the data changes shape over time, the job may suddenly begin to fail, requiring us to tweak the job's settings rather than allowing it degrade gracefully and still succeed.

	3. **All executors will need to allocate enough memory for the worst-case, which leads to over-allocation.**  When dealing with skewed data, only a few keys may require a large amount of memory to complete the join.  These keys could end up being handled by any executor, so we need to ensure that *all* executors are configured with enough memory to satisfy the worst-case, even if the majority of executors might never encounter a key that requires a significant amount of memory.

	4. **Sometimes we'd like to use fewer cluster resources at the expense of a longer job run time.**  In some cases, we don't need a job to run as fast as possible and we'd like to trade cluster utilization for execution time.  The merge-join operators will try to buffer as many values as possible in-memory before spilling to disk for better performance, but don't actually require that any values be buffered at all.  If a job's executors are configured with less memory, it may cause more data to spill to disk which will cause the job to run longer, but sometimes this is a decent trade-off as long as the job still completes within an acceptable timeframe while making more memory available for other jobs.

3. **Implement the solution differently to avoid the join.**  Sure, sometimes there may be another way to solve the problem, but you may be sacrificing developer time, performance, and/or readability to do so.  There could also be cases where there isn't a viable work-around and are forced to abandon Spark and resort to using another framework.

#### Ok, got it. So how does this solve all of that?

This library provides complementary merge-join operators for each of the built-in join operators, but  will degrade gracefully rather than fail the job if there's not enough executor memory configured to contain all of the values for any given key.  It does this by using spillable collection for the right side of the join, and will buffer values in-memory until the executor's memory threshold is exceeded and then it will start spilling to disk.

**For better performance, the larger of the two RDD's being joined should be on the LEFT side so that the number of spills occurring with the right side is minimized.**


## Requirements

This library has different versions based on the Spark version used due to changes in the internals of Spark between these versions.  It is also cross-published for Scala 2.10, so 2.10 users should replace 2.11 with 2.10 in the commands listed below.

| Spark Version | Merge-Join Version |
|---------------|-----------------|
| 2.0.x | 2.0.1 |
| 1.6.x | 1.6.1 |
| 1.5.x | 1.5.1 |


### For Spark 2.0

Using SBT:

```
libraryDependencies += "com.hindog.spark" %% "spark-mergejoin" % "2.0.1"
```

Using Maven:

```xml
<dependency>
    <groupId>com.hindog.spark</groupId>
    <artifactId>spark-mergejoin_2.11</artifactId>
    <version>2.1.0</version>
</dependency>
```

### For Spark 1.6

Using SBT:

```
libraryDependencies += "com.hindog.spark" %% "spark-mergejoin" % "1.6.1"
```

Using Maven:

```xml
<dependency>
    <groupId>com.hindog.spark</groupId>
    <artifactId>spark-mergejoin_2.11</artifactId>
    <version>1.6.1</version>
</dependency>
```

### With `spark-shell` or `spark-submit`

This library can also be added to Spark jobs launched through `spark-shell` or `spark-submit` by using the `--packages` command line option.
For example, to include it when starting the spark shell:

```
$ bin/spark-shell --packages com.hindog.spark:spark-mergejoin_2.11:2.0.0
```

Unlike using `--jars`, using `--packages` ensures that this library and its dependencies will be added to the classpath. The `--packages` argument can also be used with `bin/spark-submit`.


## Usage

#### Import

```scala
import com.hindog.spark.rdd._
```
#### Operators

The table below lists each of the standard join operators available for type `RDD[(K, V)]` and their corresponding merge-join operator provided by this library.  **One additional requirement for the merge-join operators is that an implicit **`Ordering[K]`** must be in scope.**


| Standard Join | Merge-Join Equivalent |
|---------------|----------------------|
| `join`        | `mergeJoin`          |
| `leftOuterJoin` | `leftOuterMergeJoin` |
| `rightOuterJoin` | `rightOuterMergeJoin` |
| `fullOuterJoin` | `fullOuterMergeJoin` |

For further documentation, refer to the [API docs](https://hindog.github.io/spark-mergejoin/latest/api/#com.hindog.spark.rdd.PairRDDFunctions).

#### Example
Here is an example demonstrating some joins using sample data:

```scala
val sc: SparkContext = { /\* ... create SparkContext ... \*/ }

val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))

val innerJoined = rdd1.mergeJoin(rdd2)
innerJoined.collect.foreach(println)
//  (1,(1,x))
//	(1,(2,x))
//	(2,(1,y))
//	(2,(1,z))

val leftJoined = rdd1.leftOuterJoin(rdd2)
leftJoined.collect.foreach(println)
//	(1,(1,Some(x)))
//	(1,(2,Some(x)))
//	(2,(1,Some(y)))
//	(2,(1,Some(z)))
//	(3,(1,None))

val rightJoined = rdd1.rightOuterJoin(rdd2)
rightJoined.collect.foreach(println)
//	(4,(None,w))
//	(1,(Some(1),x))
//	(1,(Some(2),x))
//	(2,(Some(1),y))
//	(2,(Some(1),z))

val fullJoined = rdd1.fullOuterMergeJoin(rdd2)
fullJoined.collect.foreach(println)
//	(4,(None,Some(w)))
//	(1,(Some(1),Some(x)))
//	(1,(Some(2),Some(x)))
//	(2,(Some(1),Some(y)))
//	(2,(Some(1),Some(z)))
//	(3,(Some(1),None))

```

#### SparkConf Options

| Key | Type | Default | Description |
|-----|-------|-------------|--------|
| `spark.mergejoin.includeSpillMetrics` | `Boolean` | `true` | Whether to include bytes spilled to memory/disk while performing the join to each task's metrics **in addition** to the standard task metrics.  Set this to `false` if you don't want to add these metrics to the standard task metrics.

## Implementation Details

The operators return an instance [MergeJoinRDD](https://hindog.github.io/spark-mergejoin/latest/api/#com.hindog.spark.rdd.MergeJoinRDD) which delegates internally to a specific implementation of [MergeJoin.Joiner](https://hindog.github.io/spark-mergejoin/latest/api/#org.apache.spark.rdd.mergejoin.MergeJoin$$Joiner) for each join type.

There are a few optimizations in place to try to minimize the amount of work that's being done during the join:

* In cases were we have a key on one side but not the other, we skip creation of the spillable collection and output the tuples directly according to the [MergeJoin.Joiner](https://hindog.github.io/spark-mergejoin/latest/api/#org.apache.spark.rdd.mergejoin.MergeJoin$$Joiner)'s `leftOuter`/`rightOuter` method.

* When we do have a key present on both sides, we need to accumulate the values for the right side (which is why it should ideally be the "smaller" of the two sides).  To do this, we create a spillable collection for the right side that will **buffer as many values as possible in memory before spilling to disk** so we only pay the penalty for spilling to disk for keys where it is necessary.

* Since the methods for the [MergeJoin.Joiner](https://hindog.github.io/spark-mergejoin/latest/api/#org.apache.spark.rdd.mergejoin.MergeJoin$$Joiner) contract (eg: `inner`, `leftOuter`, `rightOuter`) return an `Iterator` of values for each unique key, we don't need to perform join logic on each *value iteration*-- instead we perform join logic for each *unique key iteration*.

* In cases where there are no values to emit for a particular key, the [MergeJoin.Joiner](https://hindog.github.io/spark-mergejoin/latest/api/#org.apache.spark.rdd.mergejoin.MergeJoin$$Joiner) can return an empty `Iterator` for that key which causes an immediate advance to the next key instead of emitting+filtering the output rows.  (ie: consider a naive approach that instead might have the `Joiner` emit `Option[(K, (V, W))]` for every value and rely on the caller to filter out the `None`'s)

