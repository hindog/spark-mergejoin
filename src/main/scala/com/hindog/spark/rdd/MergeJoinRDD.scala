package com.hindog.spark.rdd

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.mergejoin.MergeJoin
import org.apache.spark.serializer.Serializer
import org.apache.spark.{OneToOneDependency, ShuffleDependency, _}

import scala.Seq
import scala.collection._
import scala.reflect.ClassTag

/*
 * MergeJoinRDD used to perform a scalable pairwise merge-join between two RDDs
 */

/**
 * :: @DeveloperApi ::
 *
 * Stores partition index and references to left and right partitions to be joined
 *
 * @param index The partition index
 * @param left The left Dependency to be used in the join
 * @param right The right Dependency to be used in the join
 */
protected[rdd] case class MergeJoinPartition[K, V, W](index: Int, left: Dependency[(K, V)], right: Dependency[(K, W)]) extends Partition

/**
 * :: @DeveloperApi ::
 *
 * RDD implementation for merge-join that uses a shuffle to partition and sort by keys using an implicit [[scala.Ordering Ordering]] for `K`,
 * and then delegates to an instance of [[org.apache.spark.rdd.mergejoin.MergeJoin MergeJoin]] to perform the actual merge logic.
 *
 * There is an optimization in place to avoid a shuffle in some cases where `left` or `right` are guaranteed to be partition-sorted already (ie: via `repartitionAndSortWithinPartitions`)
 *
 * @param left The left RDD to be used in the join
 * @param right The right RDD to be used in the join
 * @param partitionJoiner A function to create the [[org.apache.spark.rdd.mergejoin.MergeJoin.Joiner Joiner]] implementation to use to perform the join
 * @param part The partitioner to use
 * @param serializer The serializer to use, otherwise use the default
 */
@DeveloperApi
class MergeJoinRDD[K: ClassTag, V: ClassTag, W: ClassTag, Out: ClassTag](
	@transient left: RDD[(K, V)],
	@transient right: RDD[(K, W)],
	partitionJoiner: (MergeJoinPartition[K, V, W], TaskContext) => MergeJoin.Joiner[K, V, W, Out],
	part: Partitioner,
	serializer: Option[Serializer] = None
)(implicit ord: Ordering[K]) extends RDD[Out](left.context, Nil) {

	override val partitioner: Option[Partitioner] = Some(part)

	override protected def getDependencies: Seq[Dependency[_]] = {
		/*
		 Try to optimize for the case where a join side already has the same partitioning+ordering and
		 create a narrow dependency.

		 The only case we can reliably optimize for this is scenario is when the immediate dependencies consist of a
		 `ShuffleDependency`. (ie: if we have a `ShuffleDependency->OneToOneDependency->MergeJoin` lineage where
		 the upstream `ShuffleDependency` satisfies the partitioning+ordering, we can't rely on it as a narrow dependency
		 because even though the partitioning is preserved, the ordering of values may have been changed)

		 NOTE: that [[scala.Ordering Ordering[K]]] has no contract for [[Object.equals]], so this will only work in cases where the [[scala.Ordering Ordering[K]]]
		 *reference* matches.  eg: two separate instances of [[scala.Ordering Ordering[K]]] with the same ordering will cause us to
		 fail the check and will we will resort to a shuffle
		*/
		def isNarrow(rdd: RDD[_]): Boolean = rdd.partitioner == Some(part) && (rdd.dependencies match {
			case (dep: ShuffleDependency[K, _, _]) :: Nil => dep.partitioner == part && dep.keyOrdering == Some(ord)
			case _ => false
		})

		def createDependency[C: ClassTag](rdd: RDD[(K, C)]): Dependency[_] = {
			if (isNarrow(rdd)) {
				new OneToOneDependency[(K, C)](rdd)
			} else {
				new ShuffleDependency[K, C, C](rdd, part, serializer.getOrElse(SparkEnv.get.serializer), Some(ord))
			}
		}

		Seq(createDependency(left), createDependency(right))
	}

	override protected def getPartitions: Array[Partition] = {
		Array.tabulate(part.numPartitions)(i => {
			MergeJoinPartition(i,
				dependencies(0).asInstanceOf[Dependency[(K, V)]],
				dependencies(1).asInstanceOf[Dependency[(K, W)]]
			)
		})
	}

	@DeveloperApi
	override def compute(part: Partition, context: TaskContext): Iterator[Out] = {
		val split = part.asInstanceOf[MergeJoinPartition[K, V, W]]

		// compute the iterator for an RDD based on it's dependency
		def computeDependency[C](rdd: RDD[(K, C)], dep: Dependency[_]): Iterator[(K, C)] = {
			dep match {
				case dep: OneToOneDependency[_] => dep.rdd.compute(dep.rdd.partitions(split.index), context).asInstanceOf[Iterator[(K, C)]]
				case dep: ShuffleDependency[_, _, _] => SparkEnv.get.shuffleManager.getReader(dep.shuffleHandle, split.index, split.index + 1, context).read().asInstanceOf[Iterator[(K, C)]]
			}
		}

		val leftItr = computeDependency(left, split.left)
		val rightItr = computeDependency(right, split.right)

		new InterruptibleIterator[Out](context, new MergeJoin[K, V, W, Out](context, leftItr, rightItr, partitionJoiner(split, context)))
	}

}
