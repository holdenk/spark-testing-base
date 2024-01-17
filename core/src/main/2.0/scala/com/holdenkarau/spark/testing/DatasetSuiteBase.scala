package com.holdenkarau.spark.testing

import org.apache.spark.rdd.RDD
import org.scalatest.Suite

import scala.reflect.ClassTag

import java.time.Duration

import org.apache.spark.sql.{DataFrame, Dataset}

trait DatasetSuiteBase extends DataFrameSuiteBase
    with DatasetSuiteBaseLike { self: Suite =>
}

trait DatasetSuiteBaseLike extends DataFrameSuiteBaseLike {

  /**
   * Check if two Datasets are equals, Datasets should have the same type.
   * This method could be customized by overriding equals method for
   * the given class type.
   */
  def assertDatasetEquals[U](expected: Dataset[U], result: Dataset[U])
                            (implicit UCT: ClassTag[U]): Unit = {
    try {
      expected.rdd.cache
      result.rdd.cache
      assert("Length not Equal", expected.rdd.count, result.rdd.count)

      val expectedIndexValue: RDD[(Long, U)] = zipWithIndex(expected.rdd)
      val resultIndexValue: RDD[(Long, U)] = zipWithIndex(result.rdd)
      val unequalRDD = expectedIndexValue.join(resultIndexValue).filter
      { case (idx, (o1, o2)) => !o1.equals(o2) }

      assertEmpty(unequalRDD.take(maxUnequalRowsToShow))
    } finally {
      expected.rdd.unpersist()
      result.rdd.unpersist()
    }
  }

  /**
    * Compares if two Datasets are equal, Datasets should have the same type.
    * When comparing inexact fields uses tol.
    *
    * @param tol          max acceptable decimal tolerance, should be less than 1.
    * @param tolTimestamp max acceptable timestamp tolerance.
    * @param customShow   unit function to customize the '''show''' method
    *                     when dataframes are not equal. IE: '''df.show(false)''' or
    *                     '''df.toJSON.show(false)'''.
    */
  def assertDatasetApproximateEquals[U]
    (expected: Dataset[U], result: Dataset[U], tol: Double, tolTimestamp: Duration,
     customShow: DataFrame => Unit = _.show())
    (implicit UCT: ClassTag[U]): Unit = {

    assertDataFrameApproximateEquals(expected.toDF, result.toDF, tol, tolTimestamp)
  }

}
