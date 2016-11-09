package com.hindog.spark.rdd

import org.apache.spark.Partitioner._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.mergejoin.MergeJoin._
import org.apache.spark.{HashPartitioner, Partitioner}

import scala.reflect.ClassTag

/**
 * Merge-join operators that provide scalable equivalents to the existing Spark RDD `join`, `leftOuterJoin`, `rightOuterJoin`, `fullOuterJoin` operators.
 *
 * Refer to the documentation for [[org.apache.spark.rdd.mergejoin.MergeJoin MergeJoin]] for implementation details.
 *
 */
class PairRDDFunctions[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) extends Serializable {

	/**
	 * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
	 * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
	 * (k, v2) is in `other`. Uses the given Partitioner to partition the output RDD.
	 *
	 * During the join, values for any given (k) that is present in the right side will accumulate into memory
	 * and spilled to disk, if necessary, so they may be iterated across for every value of (k) in `this`.
	 *
	 * For performance reasons, the side of the join that has the largest number of values per unique key grouping, on average,
	 * should be `this` and joined against `other` so that the likelihood of a spill occurring with `other` will be reduced.
	 */
	def mergeJoin[W: ClassTag](other: RDD[(K, W)], partitioner: Partitioner)(implicit ord: Ordering[K]): RDD[(K, (V, W))] = {
		new MergeJoinRDD[K, V, W, (K, (V, W))](rdd, other, (part, context) => new InnerJoin[K, V, W](context), partitioner)
	}

	/**
	 * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
	 * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
	 * (k, v2) is in `other`.  Uses `org.apache.spark.HashPartitioner` to partition the results into {partitions}
	 * partitions.
	 *
	 * During the join, values for any given (k) that is present in the right side will accumulate into memory
	 * and (if necessary) spilled to disk so they may be iterated across for every value of (k) in `this`.
	 * There is no accumulation of values for `this`, only `other`.
	 *
	 * For performance reasons, the side of the join that has the largest number of values per unique key grouping, on average,
	 * should be `this` and joined against `other` so that the likelihood of a spill occurring with `other` will be reduced.
	 */
	def mergeJoin[W: ClassTag](other: RDD[(K, W)], partitions: Int)(implicit ord: Ordering[K]): RDD[(K, (V, W))] = {
		new MergeJoinRDD[K, V, W, (K, (V, W))](rdd, other, (part, context) => new InnerJoin[K, V, W](context), new HashPartitioner(partitions))
	}

	/**
	 * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
	 * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
	 * (k, v2) is in `other`. Uses the default Partitioner to partition the results.
	 *
	 * During the join, values for any given (k) that is present in the right side will accumulate into memory
	 * and (if necessary) spilled to disk so they may be iterated across for every value of (k) in `this`.
	 * There is no accumulation of values for `this`, only `other`.
	 *
	 * For performance reasons, the side of the join that has the largest number of values per unique key grouping, on average,
	 * should be `this` and joined against `other` so that the likelihood of a spill occurring with `other` will be reduced.
	 */
	def mergeJoin[W: ClassTag](other: RDD[(K, W)])(implicit ord: Ordering[K]): RDD[(K, (V, W))] = {
		new MergeJoinRDD[K, V, W, (K, (V, W))](rdd, other, (part, context) => new InnerJoin[K, V, W](context), defaultPartitioner(rdd, other))
	}

	/**
	 * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
	 * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
	 * pair (k, (v, None)) if no elements in `other` have key k. Uses the given Partitioner to
	 * partition the output RDD.
	 *
	 * During the join, values for any given (k) that is present in the right side will accumulate into memory
	 * and (if necessary) spilled to disk so they may be iterated across for every value of (k) in `this`.
	 * There is no accumulation of values for `this`, only `other`.
	 *
	 * For performance reasons, consider using `other.rightOuterMergeJoin(this)` if `other` is the larger of the two
	 * RDDs being joined.
	 */
	def leftOuterMergeJoin[W: ClassTag](other: RDD[(K, W)], partitioner: Partitioner)(implicit ord: Ordering[K]): RDD[(K, (V, Option[W]))] = {
		new MergeJoinRDD[K, V, W, (K, (V, Option[W]))](rdd, other, (part, context) => new LeftOuterJoin[K, V, W](context), partitioner)
	}

	                   
	/**
	 * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
	 * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
	 * pair (k, (v, None)) if no elements in `other` have key k. Uses `org.apache.spark.HashPartitioner`
	 * to partition the results into {partitions} partitions.
	 *
	 * During the join, values for any given (k) that is present in the right side will accumulate into memory
	 * and (if necessary) spilled to disk so they may be iterated across for every value of (k) in `this`.
	 * There is no accumulation of values for `this`, only `other`.
	 *
	 * For performance reasons, consider using `other.rightOuterMergeJoin(this)` if `other` is the larger of the two
	 * RDDs being joined.
	 */
	def leftOuterMergeJoin[W: ClassTag](other: RDD[(K, W)], partitions: Int)(implicit ord: Ordering[K]): RDD[(K, (V, Option[W]))] = {
		new MergeJoinRDD[K, V, W, (K, (V, Option[W]))](rdd, other, (part, context) => new LeftOuterJoin[K, V, W](context), new HashPartitioner(partitions))
	}

	/**
	 * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
	 * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
	 * pair (k, (v, None)) if no elements in `other` have key k. Uses the default Partitioner
	 * to partition the results.
	 *
	 * During the join, values for any given (k) that is present in the right side will accumulate into memory
	 * and (if necessary) spilled to disk so they may be iterated across for every value of (k) in `this`.
	 * There is no accumulation of values for `this`, only `other`.
	 *
	 * For performance reasons, consider using `other.rightOuterMergeJoin(this)` if `other` is the larger of the two
	 * RDDs being joined.
	 */
	def leftOuterMergeJoin[W: ClassTag](other: RDD[(K, W)])(implicit ord: Ordering[K]): RDD[(K, (V, Option[W]))] = {
		new MergeJoinRDD[K, V, W, (K, (V, Option[W]))](rdd, other, (part, context) => new LeftOuterJoin[K, V, W](context), defaultPartitioner(rdd, other))
	}

	/**
	 * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
	 * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
	 * pair (k, (None, w)) if no elements in `this` have key k. Uses the given Partitioner to
	 * partition the output RDD.
	 * 
	 * During the join, values for any given (k) that is present in the right side will accumulate into memory
	 * and (if necessary) spilled to disk so they may be iterated across for every value of (k) in `this`.
	 * There is no accumulation of values for `this`, only `other`.
	 *
	 * For performance reasons, consider using `other.leftOuterMergeJoin(this)` if `other` is the larger of the two
	 * RDDs being joined.
	 */
	def rightOuterMergeJoin[W: ClassTag](other: RDD[(K, W)], partitioner: Partitioner)(implicit ord: Ordering[K]): RDD[(K, (Option[V], W))] = {
		new MergeJoinRDD[K, V, W, (K, (Option[V], W))](rdd, other, (part, context) => new RightOuterJoin[K, V, W](context), partitioner)
	}

	/**
	 * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
	 * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
	 * pair (k, (None, w)) if no elements in `this` have key k. Uses the `org.apache.spark.HashPartitioner` to
	 * partition the results.
	 *
	 * During the join, values for any given (k) that is present in the right side will accumulate into memory
	 * and (if necessary) spilled to disk so they may be iterated across for every value of (k) in `this`.
	 * There is no accumulation of values for `this`, only `other`.
	 *
	 * For performance reasons, consider using `other.leftOuterMergeJoin(this)` if `other` is the larger of the two
	 * RDDs being joined.
	 */
	def rightOuterMergeJoin[W: ClassTag](other: RDD[(K, W)], partitions: Int)(implicit ord: Ordering[K]): RDD[(K, (Option[V], W))] = {
		new MergeJoinRDD[K, V, W, (K, (Option[V], W))](rdd, other, (part, context) => new RightOuterJoin[K, V, W](context), new HashPartitioner(partitions))
	}


	/**
	 * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
	 * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
	 * pair (k, (None, w)) if no elements in `this` have key k. Uses the default Partitioner to
	 * partition the results.
	 *
	 * During the join, values for any given (k) that is present in the right side will accumulate into memory
	 * and (if necessary) spilled to disk so they may be iterated across for every value of (k) in `this`.
	 * There is no accumulation of values for `this`, only `other`.
	 *
	 * For performance reasons, consider using `other.leftOuterMergeJoin(this)` if `other` is the larger of the two
	 * RDDs being joined.
	 */
	def rightOuterMergeJoin[W: ClassTag](other: RDD[(K, W)])(implicit ord: Ordering[K]): RDD[(K, (Option[V], W))] = {
		new MergeJoinRDD[K, V, W, (K, (Option[V], W))](rdd, other, (part, context) => new RightOuterJoin[K, V, W](context), defaultPartitioner(rdd, other))
	}

	/**
	 * Perform a full outer join of `this` and `other`. For each element (k, v) in `this`, the
	 * resulting RDD will either contain all pairs (k, (Some(v), Some(w))) for w in `other`, or
	 * the pair (k, (Some(v), None)) if no elements in `other` have key k. Similarly, for each
	 * element (k, w) in `other`, the resulting RDD will either contain all pairs
	 * (k, (Some(v), Some(w))) for v in `this`, or the pair (k, (None, Some(w))) if no elements
	 * in `this` have key k. Uses the given Partitioner to partition the output RDD.
	 *
	 * During the join, values for any given (k) that is present in the right side will accumulate into memory
	 * and spilled to disk, if necessary, so they may be iterated across for every value of (k) in `this`.
	 *
	 * For performance reasons, the side of the join that has the largest number of values per unique key grouping, on average,
	 * should be `this` and joined against `other` so that the likelihood of a spill occurring with `other` will be reduced.
	 */
	def fullOuterMergeJoin[W: ClassTag](other: RDD[(K, W)], partitioner: Partitioner)(implicit ord: Ordering[K]): RDD[(K, (Option[V], Option[W]))] = {
		new MergeJoinRDD[K, V, W, (K, (Option[V], Option[W]))](rdd, other, (part, context) => new FullOuterJoin[K, V, W](context), partitioner)
	}


	/**
	 * Perform a full outer join of `this` and `other`. For each element (k, v) in `this`, the
	 * resulting RDD will either contain all pairs (k, (Some(v), Some(w))) for w in `other`, or
	 * the pair (k, (Some(v), None)) if no elements in `other` have key k. Similarly, for each
	 * element (k, w) in `other`, the resulting RDD will either contain all pairs
	 * (k, (Some(v), Some(w))) for v in `this`, or the pair (k, (None, Some(w))) if no elements
	 * in `this` have key k. Uses the `org.apache.spark.HashPartitioner` to partition the results.
	 *
	 * During the join, values for any given (k) that is present in the right side will accumulate into memory
	 * and spilled to disk, if necessary, so they may be iterated across for every value of (k) in `this`.
	 *
	 * For performance reasons, the side of the join that has the largest number of values per unique key grouping, on average,
	 * should be `this` and joined against `other` so that the likelihood of a spill occurring with `other` will be reduced.
	 */
	def fullOuterMergeJoin[W: ClassTag](other: RDD[(K, W)], partitions: Int)(implicit ord: Ordering[K]): RDD[(K, (Option[V], Option[W]))] = {
		new MergeJoinRDD[K, V, W, (K, (Option[V], Option[W]))](rdd, other, (part, context) => new FullOuterJoin[K, V, W](context), new HashPartitioner(partitions))
	}


	/**
	 * Perform a full outer join of `this` and `other`. For each element (k, v) in `this`, the
	 * resulting RDD will either contain all pairs (k, (Some(v), Some(w))) for w in `other`, or
	 * the pair (k, (Some(v), None)) if no elements in `other` have key k. Similarly, for each
	 * element (k, w) in `other`, the resulting RDD will either contain all pairs
	 * (k, (Some(v), Some(w))) for v in `this`, or the pair (k, (None, Some(w))) if no elements
	 * in `this` have key k. Uses the default Partitioner to partition the output RDD.
	 *
	 * During the join, values for any given (k) that is present in the right side will accumulate into memory
	 * and spilled to disk, if necessary, so they may be iterated across for every value of (k) in `this`.
	 *
	 * For performance reasons, the side of the join that has the largest number of values per unique key grouping, on average,
	 * should be `this` and joined against `other` so that the likelihood of a spill occurring with `other` will be reduced.
	 */
	def fullOuterMergeJoin[W: ClassTag](other: RDD[(K, W)])(implicit ord: Ordering[K]): RDD[(K, (Option[V], Option[W]))] = {
		new MergeJoinRDD[K, V, W, (K, (Option[V], Option[W]))](rdd, other, (part, context) => new FullOuterJoin[K, V, W](context), defaultPartitioner(rdd, other))
	}		
}

