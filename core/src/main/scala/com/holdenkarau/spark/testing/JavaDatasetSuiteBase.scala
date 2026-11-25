package com.holdenkarau.spark.testing

import org.apache.spark.sql.Dataset

class JavaDatasetSuiteBase extends JavaDataFrameSuiteBase
    with DatasetSuiteBaseLike with Serializable {

  /**
   * Check if two Datasets are equals, Datasets should have the same type.
   * This method could be customized by overriding equals method for
   * the given class type.
   */
  def assertDatasetEquals[U](expected: Dataset[U], result: Dataset[U]): Unit = {
    super.assertDatasetEquals(expected, result)(Utils.fakeClassTag[U])
  }

  /**
    * Compares if two Datasets are equal, Datasets should have the same type.
    * When comparing inexact fields uses tol.
    *
    * @param tol max acceptable tolerance, should be less than 1.
    */
  def assertDatasetApproximateEquals[U]
    (expected: Dataset[U], result: Dataset[U], tol: Double): Unit = {
    super.assertDatasetApproximateEquals(
      expected, result, tol)(Utils.fakeClassTag[U])
  }
}
