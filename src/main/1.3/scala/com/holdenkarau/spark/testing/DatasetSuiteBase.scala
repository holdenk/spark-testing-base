package com.holdenkarau.spark.testing

import org.apache.spark.sql.Dataset

import scala.reflect.ClassTag

class DatasetSuiteBase extends DataFrameSuiteBase {

  /**
    * Check if two Datasets are equals, Datasets should have the same type.
    * This method could be customized by overriding equals method for the given class type.
    */
  def equalDatasets[U: ClassTag, V: ClassTag](expected: Dataset[U], result: Dataset[V]) = {
    assert(implicitly[ClassTag[U]].runtimeClass == implicitly[ClassTag[V]].runtimeClass)

    val expectedRDD = zipWithIndex(expected.rdd)
    val resultRDD = zipWithIndex(result.rdd)

    try {
      expectedRDD.cache()
      resultRDD.cache()
      assert(expectedRDD.count() == expectedRDD.count())

      val unequalRDD = expectedRDD.join(resultRDD).filter { case (idx, (o1, o2)) => !o1.equals(o2) }

      assert(unequalRDD.isEmpty())
    } finally {
      expectedRDD.unpersist()
      resultRDD.unpersist()
    }
  }
}
