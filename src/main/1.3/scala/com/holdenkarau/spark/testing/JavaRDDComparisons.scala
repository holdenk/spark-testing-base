package com.holdenkarau.spark.testing

import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}

object JavaRDDComparisons extends RDDComparisonsLike with JavaTestSuite {

  /**
   * Asserts two RDDs are equal (with the same order).
   * If they are equal assertion succeeds, otherwise assertion fails.
   */
  def assertRDDEqualsWithOrder[T](expected: JavaRDD[T], result: JavaRDD[T]): Unit = {
    assertTrue(compareRDDWithOrder(expected, result).isEmpty)
  }

  /**
   * Compare two RDDs with order (e.g. [1,2,3] != [3,2,1]).
   * If the partitioners are not the same this requires multiple passes on the input.
   * If they are equal returns None, otherwise returns Some with the first mismatch.
   * If the lengths are not equal, one of the two components may be None.
   */
  def compareRDDWithOrder[T](expected: JavaRDD[T], result: JavaRDD[T]):
      Option[(Option[T], Option[T])] = {
    implicit val ctag = Utils.fakeClassTag[T]
    compareRDDWithOrder(expected.rdd, result.rdd)
  }

  /**
   * Asserts two RDDs are equal (with the same order).
   * If they are equal assertion succeeds, otherwise assertion fails.
   */
  def assertPairRDDEqualsWithOrder[K, V](expected: JavaPairRDD[K, V], result: JavaPairRDD[K, V]): Unit = {
    assertTrue(compareWithOrder(expected, result).isEmpty)
  }

  /**
   * Compare two RDDs with order (e.g. [1,2,3] != [3,2,1]).
   * If the partitioners are not the same this requires multiple passes on the input.
   * If they are equal returns None, otherwise returns Some with the first mismatch.
   * If the lengths are not equal, one of the two components may be None.
   */
  def compareWithOrder[K, V](expected: JavaPairRDD[K, V], result: JavaPairRDD[K, V]):
      Option[(Option[(K, V)], Option[(K, V)])] = {

    implicit val ctag = Utils.fakeClassTag[(K, V)]
    RDDComparisons.compareWithOrder(expected.rdd, result.rdd)
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

  /**
   * Asserts two RDDs are equal (un ordered).
   * If they are equal assertion succeeds, otherwise assertion fails.
   */
  def assertPairRDDEquals[K, V](expected: JavaPairRDD[K, V], result: JavaPairRDD[K, V]): Unit = {
    assertTrue(compare(expected, result).isEmpty)
  }

  /**
   * Compare two Pair RDDs where we do not require the order to be equal.
   * If they are equal returns None, otherwise returns Some with the first mismatch.
   *
   * @return None if the two RDDs are equal, or Some That contains first mismatch information.
   *         Mismatch information will be Tuple3 of: ((Key, Value), number of times this key occur in expected RDD,
   *         number of times this key occur in result RDD)
   */
  def compare[K, V](expected: JavaPairRDD[K, V], result: JavaPairRDD[K, V]): Option[((K, V), Integer, Integer)] = {
    implicit val ctag = Utils.fakeClassTag[(K, V)]
    RDDComparisons.compare(expected.rdd, result.rdd)
      .map(x => (x._1, Integer.valueOf(x._2), Integer.valueOf(x._3)))
  }
}
