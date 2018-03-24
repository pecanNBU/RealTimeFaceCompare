package com.hzgc.cluster.clustering

import com.hzgc.jni.ClusteringFunction
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD._
import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]): Unit = {
    println(ClusteringFunction.clusteringComputer(new Array[Float](0),1,"test.txt","/opt"))
    println(System.getProperty("java.library.path"))


    //    val spark = SparkSession.builder().appName("test").master("local[2]").enableHiveSupport().getOrCreate()
//    // Create an RDD of key-value pairs with Long keys.
//    val rdd = spark.sparkContext.parallelize((1 to 100).map(x => (x, 0)))
//    // Construct an IndexedRDD from the pairs, hash-partitioning and indexing
//    // the entries.
//    var indexed = IndexedRDD(rdd).cache()
//    indexed = indexed.put(100, 10873)
//    val array = new Array[Int](1)
//    array(0) = 99
//    indexed = indexed.delete(array)
//    println(indexed.get(100))
//    println(indexed.get(99))
    /*// Perform a point update.
    val indexed2 = indexed.put(1234L, 10873).cache()
    // Perform a point lookup. Note that the original IndexedRDD remains
    // unmodified.
    indexed2.get(1234L) // => Some(10873)
    indexed.get(1234L) // => Some(0)

    // Efficiently join derived IndexedRDD with original.
    val indexed3 = indexed.innerJoin(indexed2) { (id, a, b) => b }.filter(_._2 != 0)
    indexed3.collect // => Array((1234L, 10873))

    // Perform insertions and deletions.
    val indexed4 = indexed2.put(-100L, 111).delete(Array(998L, 999L)).cache()
    indexed2.get(-100L) // => None
    indexed4.get(-100L) // => Some(111)
    indexed2.get(999L) // => Some(0)
    indexed4.get(999L) // => None*/

  }
}
