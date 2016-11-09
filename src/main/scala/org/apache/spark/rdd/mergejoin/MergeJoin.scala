package org.apache.spark.rdd.mergejoin

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.Logging
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter
import org.apache.spark.{InterruptibleIterator, SparkEnv, TaskContext}

import scala.Iterator
import scala.annotation.switch
import scala.collection._
import scala.util.Try
import scala.util.control.NonFatal

/**
 * :: DeveloperApi ::
 *
 * Merge-join implementation that will create a spill-able collection for the right-side to be iterated over
 * for each matching key on the left side.  This enables joins that don't require that values for any
 * given key to be required to fit in memory, but it *will* try to buffer as many values as possible using
 * Spark's built-in 'ExternalSorter', and it's a private class so that's why this class is packaged here.
 *
 * There are numerous optimizations in place to try to minimize the work being done in the join:
 *
 *  1) Since the [[org.apache.spark.rdd.mergejoin.MergeJoin.Joiner Joiner]] returns a value [[scala.Iterator Iterator]] for each key, we don't need to invoke join logic on each
 *    value iteration-- instead we perform join logic for each unique key.  This also allows each 'Joiner' to
 *    optimize via the methods below based on the join being performed.
 *
 *  2) In cases were we have a key on one side but not the other, we skip creation of the spillable collection
 *    and write the output tuples directly according to the [[org.apache.spark.rdd.mergejoin.MergeJoin.Joiner Joiner]]'s `leftOuter`/`rightOuter` method.
 *
 *  3) In cases where there are no values to emit for a particular key, the [[org.apache.spark.rdd.mergejoin.MergeJoin.Joiner Joiner]] can emit an empty [[scala.Iterator Iterator]],
 *    in which case we will immediately move to the next key without emitting+filtering tuples for those values.
 *
 *  4) In cases where we have a key on both sides, we invoke the [[org.apache.spark.rdd.mergejoin.MergeJoin.Joiner Joiner]]'s `inner` method.  The default implementation will create a spill-able collection for the right
 *    side that will buffer as many values as possible in memory before spilling to disk... so we only pay the
 *    penalty for spilling to disk on keys where it is absolutely necessary.
 *
 * @param context The TaskContext we are executing within
 * @param left The left side of the join, pre-ordered by `Ordering[K]`
 * @param right The right side of the join, pre-ordered by `Ordering[K]`
 */
@DeveloperApi
class MergeJoin[K, V, W, O](
	context: TaskContext,
	left: Iterator[(K, V)],
	right: Iterator[(K, W)],
	joiner: MergeJoin.Joiner[K, V, W, O])(implicit protected val ord: Ordering[K])
extends Iterator[O]
{

	protected var finished: Boolean = false

	// base iterators for each side
	protected var leftRemaining: BufferedIterator[(K, V)] = left.buffered
	protected var rightRemaining: BufferedIterator[(K, W)] = right.buffered

	// current value iterator for the current key
	protected var currentIterator: Iterator[O] = Iterator.empty

	// returns an iterator for the current left values for 'key' and updates 'leftRemaining' to point to the start of the next key
	protected def takeLeftValuesForKey(key: K): Iterator[(K, V)] = {
		val (forKey, tail) = leftRemaining.span(kv => ord.equiv(kv._1, key))
		leftRemaining = tail.buffered
		forKey
	}

	// returns an iterator for the current right values for 'key' and updates 'rightRemaining' to point to the start of the next key
	protected def takeRightValuesForKey(key: K): Iterator[(K, W)] = {
		val (forKey, tail) = rightRemaining.span(kv => ord.equiv(kv._1, key))
		rightRemaining = tail.buffered
		forKey
	}

	override def hasNext: Boolean = {
		// loop until we get a non-empty value iterator or finished
		while (!currentIterator.hasNext) {
			currentIterator = nextIterator()
			// we check finished here and break with local return so that we don't need to check on every iteration
			if (finished) {
				return false
			}
		}

		true
	}

	override def next(): O = currentIterator.next()

	protected def finish: Iterator[O] = {
		finished = true
		Iterator.empty
	}

	protected def nextIterator(): Iterator[O] = {

		import MergeJoin.State._

		// check to see which sides have remaining values and create a 'state' value that
		// can be used in a jump table
		var state = 0
		if (leftRemaining.hasNext) state = state | LeftRemaining
		if (rightRemaining.hasNext) state = state | RightRemaining

		(state: @switch) match {
			case BothRemaining =>
				val left = leftRemaining.head
				val right = rightRemaining.head

				ord.compare(left._1, right._1) match {
					// left is behind, write values for current left key
					case i if i < 0 => joiner.leftOuter(takeLeftValuesForKey(left._1))

					// right is behind, write values for current right key
					case i if i > 0 => joiner.rightOuter(takeRightValuesForKey(right._1))

					// both sides equal, write values for both sides
					case _ => joiner.inner(left._1, takeLeftValuesForKey(left._1).map(_._2), takeRightValuesForKey(right._1).map(_._2))
				}

			case LeftRemaining =>
				// drain the remaining left side
				// if the joiner returns an empty Iterator, then we are finished
				joiner.leftOuter(leftRemaining) match {
					case i if i.hasNext => i
					case _ => finish
				}

			case RightRemaining =>
				// drain the remaining right side
				// if the joiner returns an empty Iterator, then we are finished
				joiner.rightOuter(rightRemaining) match {
					case i if i.hasNext => i
					case _ => finish
				}

			case NoneRemaining => finish
		}
	}
}


object MergeJoin {

	protected val confKeyIncludeSpillMetrics = "spark.mergejoin.includeSpillMetrics"

	/*
		Constants to represent the state of iterators on both sides of the join.
		Allows for jump table rather than decision tree when deciding what to do next
	 */
	protected object State {
		final val NoneRemaining   = 0
		final val LeftRemaining   = 1
		final val RightRemaining  = 2
		final val BothRemaining   = 3
	}

	/**
	 * Implementation contract for a merge-join.
	 *
	 * `inner` is called when there are matching keys in both sides
	 *
	 * `leftOuter` is called when a key exists only in the left side
	 *
	 * `rightOuter` is called when a key exists only in the right side
	 */
	trait Joiner[K, V, W, Out] extends Serializable {
		/**
		 * Emit both `left` and `right` values for the given `key`
		 */
		def inner(key: K, left: Iterator[V], right: Iterator[W]): Iterator[Out]
		/**
		 * Emit values for left-side only key
		 */
		def leftOuter(itr: Iterator[(K, V)]): Iterator[Out]
		/**
		 * Emit values for right-side only key
		 */
		def rightOuter(itr: Iterator[(K, W)]): Iterator[Out]
	}

	/**
	 * Base implementation for spillable merge-join [[Joiner]] that contains a default implementation for
	 * `inner` that will accumulate all right values into a spillable collection.
	 *
	 * Exposes an `emit` method to allow each implementation to format the output tuple accordingly
	 */
	abstract class SpillableJoiner[K, V, W, Out](context: TaskContext) extends Joiner[K, V, W, Out] with Logging {

		context.addTaskCompletionListener{ context => closeKey() }

		protected var currentKey: K = _
		protected var currentSpillable: ExternalSorter[Int, W, W] = _

		protected val includeSpillMetrics: Boolean = SparkEnv.get.conf.getBoolean(confKeyIncludeSpillMetrics, true)

		/**
		 * @param key key value to emit
		 * @param left left value to emit
		 * @param right right value to emit
		 * @return output value for a single row, eg: `(K, (V, W))` or `(K, (V, Option[W]))`
		 */
		def emit(key: K, left: V, right: W): Out

		override def inner(key: K, left: Iterator[V], right: Iterator[W]): Iterator[Out] = {
			// try to close previous iteration (just in case the iterator wasn't completed for whatever reason)
			closeKey()

			currentKey = key

			val rightSpillable = {
				currentSpillable = new ExternalSorter[Int, W, W](None, None, Some(implicitly[Ordering[Int]]))
				currentSpillable.insertAll(new InterruptibleIterator(context, right.zipWithIndex.map(_.swap)))

				new Iterable[W] {
					override def iterator: Iterator[W] = new InterruptibleIterator(context, currentSpillable.iterator.map(_._2))
				}
			}

			val itr = for {
				l <- left
				r <- rightSpillable.iterator
			} yield emit(key, l, r)

			CompletionIterator[Out, Iterator[Out]](itr, closeKey())
		}

		/**
		 * Called after all values for a key has been emitted.
		 *
		 * This will be called after each 'inner' key join, and once more on task completion to ensure
		 * we cleanup any intermediate spill files and release memory back to the executor when we
		 * are done with this key.
		 */
		protected def closeKey(): Unit = {
			if (currentSpillable != null) {
				Try {
					currentSpillable.stop()

					if (includeSpillMetrics) {
						context.taskMetrics().incDiskBytesSpilled(currentSpillable.diskBytesSpilled)
						context.taskMetrics().incMemoryBytesSpilled(currentSpillable.memoryBytesSpilled)
					}
				}.recover{ case NonFatal(ex) => logWarning(s"Exception while closing external collection.  Please open an issue at https://github.com/hindog/spark-mergejoin/issues", ex) }

				currentKey = null.asInstanceOf[K]
				currentSpillable = null
			}
		}

		override def finalize(): Unit = {
			// safety-check to ensure we don't throw any exceptions here (ie: while logging)
			Try {
				if (currentSpillable != null) {
					logWarning("External collection still open on finalizer call of merge-join, this could mean we are leaking resources.  Please open an issue at https://github.com/hindog/spark-mergejoin/issues")
				}
			}.recover{ case NonFatal(ex) => /* should really never happen */ }
		}
	}

	class InnerJoin[K, V, W](context: TaskContext, serializer: Option[Serializer] = None) extends SpillableJoiner[K, V, W, (K, (V, W))](context) {
		override def leftOuter(itr: Iterator[(K, V)]): Iterator[(K, (V, W))] = Iterator.empty
		override def rightOuter(itr: Iterator[(K, W)]): Iterator[(K, (V, W))] = Iterator.empty
		override def emit(key: K, left: V, right: W): (K, (V, W)) = key -> (left, right)
	}

	class LeftOuterJoin[K, V, W](context: TaskContext, serializer: Option[Serializer] = None) extends SpillableJoiner[K, V, W, (K, (V, Option[W]))](context) {
		override def leftOuter(itr: Iterator[(K, V)]): Iterator[(K, (V, Option[W]))] = itr.map{ case (k, v) => k -> (v, None) }
		override def rightOuter(itr: Iterator[(K, W)]): Iterator[(K, (V, Option[W]))] = Iterator.empty
		override def emit(key: K, left: V, right: W): (K, (V, Option[W])) = key -> (left, Some(right))
	}

	class RightOuterJoin[K, V, W](context: TaskContext, serializer: Option[Serializer] = None) extends SpillableJoiner[K, V, W, (K, (Option[V], W))](context) {
		override def leftOuter(itr: Iterator[(K, V)]): Iterator[(K, (Option[V], W))] = Iterator.empty
		override def rightOuter(itr: Iterator[(K, W)]): Iterator[(K, (Option[V], W))] = itr.map{ case (k, v) => k -> (None, v) }
		override def emit(key: K, left: V, right: W): (K, (Option[V], W)) = key -> (Some(left), right)
	}

	class FullOuterJoin[K, V, W](context: TaskContext, serializer: Option[Serializer] = None) extends SpillableJoiner[K, V, W, (K, (Option[V], Option[W]))](context) {
		override def leftOuter(itr: Iterator[(K, V)]): Iterator[(K, (Option[V], Option[W]))] = itr.map{ case (k, v) => k -> (Some(v), None) }
		override def rightOuter(itr: Iterator[(K, W)]): Iterator[(K, (Option[V], Option[W]))] = itr.map{ case (k, v) => k -> (None, Some(v)) }
		override def emit(key: K, left: V, right: W): (K, (Option[V], Option[W])) = key -> (Some(left), Some(right))
	}

}

