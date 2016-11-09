package com.hindog.spark.rdd

import org.apache.spark.{HashPartitioner, OneToOneDependency, SharedSparkContext, ShuffleDependency}
import org.scalatest.FunSuite

import scala.collection._

class PairRDDFunctionsSuite extends FunSuite with SharedSparkContext {

	test("mergeJoin") {
		val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
		val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
		assertResult(rdd1.join(rdd2).collect().toSet)(rdd1.mergeJoin(rdd2).collect().toSet)
	}

	test("mergeJoin all-to-all") {
		val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (1, 3)))
		val rdd2 = sc.parallelize(Array((1, 'x'), (1, 'y')))
		assertResult(rdd1.join(rdd2).collect().toSet)(rdd1.mergeJoin(rdd2).collect().toSet)
	}

	test("leftOuterMergeJoin") {
		val rdd1 = sc.parallelize(Array((1, 2), (2, 1), (3, 1), (1, 1)))
		val rdd2 = sc.parallelize(Array((1, 'x'), (4, 'w'), (2, 'y'), (2, 'z')))
		assertResult(rdd1.leftOuterJoin(rdd2).collect().toSet)(rdd1.leftOuterMergeJoin(rdd2).collect().toSet)
	}

	test("rightOuterMergeJoin") {
		val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
		val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
		assertResult(rdd1.rightOuterJoin(rdd2).collect().toSet)(rdd1.rightOuterMergeJoin(rdd2).collect().toSet)
	}

	test("fullOuterMergeJoin") {
		val rdd1 = sc.parallelize(Array((1, 2), (2, 1), (3, 1), (1, 1)))
		val rdd2 = sc.parallelize(Array((2, 'y'), (2, 'z'), (4, 'w'), (1, 'x')))
		assertResult(rdd1.fullOuterJoin(rdd2).collect().toSet)(rdd1.fullOuterMergeJoin(rdd2).collect().toSet)
	}

	test("mergeJoin with no matches") {
		val rdd1 = sc.parallelize(Array((1, 2), (2, 1), (3, 1), (1, 2)))
		val rdd2 = sc.parallelize(Array((4, 'x'), (5, 'y'), (5, 'z'), (6, 'w')))
		assertResult(rdd1.join(rdd2).collect().toSet)(rdd1.mergeJoin(rdd2).collect().toSet)
	}

	test("mergeJoin with many output partitions") {
		val rdd1 = sc.parallelize(Array((1, 2), (2, 1), (3, 1), (1, 1)))
		val rdd2 = sc.parallelize(Array((2, 'y'), (2, 'z'), (4, 'w'), (1, 'x')))
		assertResult(rdd1.join(rdd2, new HashPartitioner(10)).collect().toSet)(rdd1.mergeJoin(rdd2, new HashPartitioner(10)).collect().toSet)
	}

	test("mergeJoin with narrow dependency optimization") {
		val partitioner = new HashPartitioner(10)
		val rdd1 = sc.parallelize(Array((1, 2), (2, 1), (3, 1), (1, 1)), 2).repartitionAndSortWithinPartitions(partitioner)
		val rdd2 = sc.parallelize(Array((2, 'y'), (2, 'z'), (4, 'w'), (1, 'x')), 2).partitionBy(partitioner)

		val joinedRdd = rdd1.mergeJoin(rdd2, partitioner)
		// rdd1 was already sorted and repartitioned
		assert(joinedRdd.dependencies(0).isInstanceOf[OneToOneDependency[_]])
		assert(joinedRdd.dependencies(1).isInstanceOf[ShuffleDependency[_, _, _]])

		assertResult(rdd1.join(rdd2, partitioner).collect().toSet)(joinedRdd.collect().toSet)
	}

	test("mergeJoin without narrow dependency optimization") {
		val partitioner = new HashPartitioner(10)
		val rdd1 = sc.parallelize(Array((1, 2), (2, 1), (3, 1), (1, 1))).repartitionAndSortWithinPartitions(partitioner)
									  .mapPartitions(itr => Iterator[(Int, Int)]((2, 2), (1, 2))) // negates narrow dependency
		val rdd2 = sc.parallelize(Array((2, 'y'), (2, 'z'), (4, 'w'), (1, 'x'))).partitionBy(partitioner)

		val joinedRDD = rdd1.mergeJoin(rdd2, partitioner)
		assert(joinedRDD.dependencies(0).isInstanceOf[ShuffleDependency[_, _, _]])
		assert(joinedRDD.dependencies(1).isInstanceOf[ShuffleDependency[_, _, _]])

		assertResult(rdd1.join(rdd2, partitioner).collect().toSet)(joinedRDD.collect().toSet)
	}

}
