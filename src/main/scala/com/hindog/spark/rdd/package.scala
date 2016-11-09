package com.hindog.spark

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.language.implicitConversions

/**
 * Spark merge-join capability for RDDs. See [[com.hindog.spark.rdd.PairRDDFunctions]] for further documentation
 */
package object rdd {

	implicit def toPairRDDFunctions[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): PairRDDFunctions[K, V] = new PairRDDFunctions[K, V](rdd)

}
