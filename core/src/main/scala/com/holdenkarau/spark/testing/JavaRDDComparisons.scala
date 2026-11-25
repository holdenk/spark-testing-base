package com.holdenkarau.spark.testing

import org.apache.spark.api.java.JavaRDD

object JavaRDDComparisons extends RDDComparisonsLike with JavaTestSuite {

  /**
   * Asserts two RDDs are equal (with the same order).
   * If they are equal assertion succeeds, otherwise assertion fails.
   */
  def assertRDDEqualsWithOrder[T](expected: JavaRDD[T], result: JavaRDD[T]): Unit = {
    assertTrue(compareRDDWithOrder(expected, result).isEmpty)
  }

  /**
   * Compare two RDDs. If they are equal returns None, otherwise
   * returns Some with the first mismatch. Assumes we have the same partitioner.
   */
  def compareRDDWithOrder[T](expected: JavaRDD[T], result: JavaRDD[T]):
      Option[(Option[T], Option[T])] = {
    implicit val ctag = Utils.fakeClassTag[T]
    compareRDDWithOrder(expected.rdd, result.rdd)
  }

  /**
   * Asserts two RDDs are equal (un ordered).
   * If they are equal assertion succeeds, otherwise assertion fails.
   */
  def assertRDDEquals[T](expected: JavaRDD[T], result: JavaRDD[T]): Unit = {
    assertTrue(compareRDD(expected, result).isEmpty)
  }

  /**
   * Compare two RDDs where we do not require the order to be equal.
   * If they are equal returns None, otherwise returns Some with the first mismatch.
   *
   * @return None if the two RDDs are equal, or Some that contains the first
   *         mismatch information. Mismatch information will be Tuple3 of:
   *         (key, number of times this key occur in expected RDD,
   *         number of times this key occur in result RDD)
   */
  def compareRDD[T](expected: JavaRDD[T], result: JavaRDD[T]):
      Option[(T, Integer, Integer)] = {
    implicit val ctag = Utils.fakeClassTag[T]
    compareRDD(expected.rdd, result.rdd).
      map{case(value, expectedCount, resultCount) => (value, Integer.valueOf(expectedCount), Integer.valueOf(resultCount))}
  }

}
