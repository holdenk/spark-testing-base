package com.holdenkarau.spark.testing

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

import org.apache.spark.sql.Dataset

class DatasetSuiteBase extends DataFrameSuiteBase with DatasetSuiteBaseLike {

}

class JavaDatasetSuiteBase extends JavaDataFrameSuiteBase with DatasetSuiteBaseLike with Serializable {
  def fakeClassTag[T]: ClassTag[T] = ClassTag.AnyRef.asInstanceOf[ClassTag[T]]

  def equalDatasets[U, V](expected: Dataset[U], result: Dataset[V]) = {
    super.equalDatasets(expected, result)(fakeClassTag[U], fakeClassTag[V])
  }
}

trait DatasetSuiteBaseLike extends DataFrameSuiteBaseLike {

  /**
    * Check if two Datasets are equals, Datasets should have the same type.
    * This method could be customized by overriding equals method for the given class type.
    */
  def equalDatasets[U, V](expected: Dataset[U], result: Dataset[V])
                          (implicit ctU: ClassTag[U], ctV: ClassTag[V]) = {

    try {
      expected.rdd.cache
      result.rdd.cache
      assert(expected.rdd.count == result.rdd.count)

      val expectedIndexValue: RDD[(Long, U)] = zipWithIndex(expected.rdd)
      val resultIndexValue: RDD[(Long, V)] = zipWithIndex(result.rdd)
      val unequalRDD = expectedIndexValue.join(resultIndexValue).filter
      { case (idx, (o1, o2)) => !o1.equals(o2) }

      assert(unequalRDD.take(maxUnequalRowsToShow).isEmpty)
    } finally {
      expected.rdd.unpersist()
      result.rdd.unpersist()
    }
  }

  /**
    * Compares if two Datasets are equal, Datasets should have the same type.
    * When comparing inexact fields uses tol.
    *
    * @param tol max acceptable tolerance, should be less than 1.
    */
  def approxEqualDatasets[U: ClassTag, V: ClassTag](expected: Dataset[U], result: Dataset[V], tol: Double) = {
    assert(implicitly[ClassTag[U]].runtimeClass == implicitly[ClassTag[V]].runtimeClass)

    approxEqualDataFrames(expected.toDF, result.toDF, tol)
  }

}
