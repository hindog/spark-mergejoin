package com.hindog.spark.rdd

import org.apache.spark.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

import scala.collection._
import scala.reflect.ClassTag
import scala.util.Random

class SparkMergeJoinRandomizerTest extends FunSuite with SharedSparkContext {

	def withLogLevel[T](level: String)(work: => T): T = {
		import org.apache.log4j._
		val current = Logger.getRootLogger.getLevel()
		Logger.getRootLogger.setLevel(Level.toLevel(level.toUpperCase))
		try {
			work
		} finally {
			Logger.getRootLogger.setLevel(current)
		}
	}

	implicit class RDDFunctions[T: ClassTag](rdd: RDD[T]) {

		def dump(name: String, maxRows: Int = 20) = {
			val res = rdd.take(maxRows)
			val sb = new StringBuffer()

			sb.append(s"$name: RDD[${implicitly[ClassTag[T]].runtimeClass.getSimpleName}]\n")
			sb.append(s"-------- Lineage -----------\n")
			sb.append(rdd.toDebugString + "\n")
			sb.append(s"-------- Partitions [${rdd.partitions.length}]---------\n")
			sb.append(rdd.partitions.foreach(p => sb.append(p + "\n")) + "\n")
			sb.append("--------- Data --------------\n")
			res.foreach(v => sb.append(v + "\n"))
			sb.append("... (omitted)")
			println(sb.toString.replace("\n", s"\n$name: "))
			rdd
		}
	}


	def testJoin[V: ClassTag](name: String, mergeJoin: (RDD[(Int, Int)], RDD[(Int, Int)]) => RDD[(Int, V)], join: (RDD[(Int, Int)], RDD[(Int, Int)]) => RDD[(Int, V)]): Unit = {

		val maxKeys = 20 // max number of distinct keys per RDD
		val maxValues = 1000 // max number of values per RDD
		val numJoins = 100

		val rand = new Random()

		println(s"Running $numJoins random tests of '$name' operator, this may take a few minutes...")

		// test some random joins with randomized data
		(0 until numJoins).foreach { i =>
			withLogLevel("WARN") {
				val left = (0 until rand.nextInt(maxValues)).map(i => rand.nextInt() % maxKeys -> i)
				val right = (0 until rand.nextInt(maxValues)).map(i => rand.nextInt() % maxKeys -> i)

				// also randomize the input partition count
				val rdd1 = sc.parallelize(left, rand.nextInt(10) + 1)
				val rdd2 = sc.parallelize(right, rand.nextInt(10) + 1)

				var mergeJoinResult: RDD[(Int, V)] = null
				var joinResult: RDD[(Int, V)] = null
				var expected: Set[(Int, V)] = null
				var actual: Set[(Int, V)] = null
			  try {
				  mergeJoinResult = mergeJoin(rdd1, rdd2)
				  joinResult = join(rdd1, rdd2)
				  expected = joinResult.collect().toSet
				  actual = mergeJoinResult.collect().toSet
				  assertResult(true)(expected != null)
				  assertResult(true)(actual != null)
				  assertResult(expected)(actual)
			  } catch {
				  case ex: Throwable => {
					  // dump the failed data for analysis
					  rdd1.dump("rdd1", 100000)
					  rdd2.dump("rdd2", 100000)
					  mergeJoinResult.dump(s"$name [actual]", 100000)
					  joinResult.dump(s"$name [expected]", 100000)
					  throw ex
				  }
			  }
			}
		}
	}

	test("test mergeJoin") {
		testJoin("mergeJoin", (l, r) => l.mergeJoin(r), (l, r) => l.join(r))
	}

	test("test leftOuterMergeJoin") {
		testJoin("leftOuterMergeJoin", (l, r) => l.leftOuterMergeJoin(r), (l, r) => l.leftOuterJoin(r))
	}

	test("test rightOuterMergeJoin") {
		testJoin("rightOuterMergeJoin", (l, r) => l.rightOuterMergeJoin(r), (l, r) => l.rightOuterJoin(r))
	}

	test("test fullOuterMergeJoin") {
		testJoin("fullOuterMergeJoin", (l, r) => l.fullOuterMergeJoin(r), (l, r) => l.fullOuterJoin(r))
	}

}