package com.holdenkarau.spark.testing

import org.scalatest.FunSuite

class MultipleDataFrameSuites extends FunSuite with DataFrameSuiteBase {
  test("test nothing") {
    assert(1 === 1)
  }
}
