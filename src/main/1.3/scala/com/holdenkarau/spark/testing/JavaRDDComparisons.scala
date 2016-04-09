package com.holdenkarau.spark.testing

import org.apache.spark.api.java.JavaRDD

object JavaRDDComparisons {

  /**
   * Compare two RDDs. If they are equal returns None, otherwise
   * returns Some with the first mismatch. Assumes we have the same partitioner.
   */
  def compareWithOrder[T](expected: JavaRDD[T], result: JavaRDD[T]): Option[(T, T)] = {
    implicit val ctag = Utils.fakeClassTag[T]
    RDDComparisons.compareWithOrder(expected.rdd, result.rdd)
  }

  /**
   * Compare two RDDs where we do not require the order to be equal.
   * If they are equal returns None, otherwise returns Some with the first mismatch.
   *
   * @return None if the two RDDs are equal, or Some That contains first mismatch information.
   *         Mismatch information will be Tuple3 of: (key, number of times this key occur in expected RDD,
   *         number of times this key occur in result RDD)
   */
  def compare[T](expected: JavaRDD[T], result: JavaRDD[T]): Option[(T, Integer, Integer)] = {
    implicit val ctag = Utils.fakeClassTag[T]
    RDDComparisons.compare(expected.rdd, result.rdd)
      .map(x => (x._1, Integer.valueOf(x._2), Integer.valueOf(x._3)))
  }

}
