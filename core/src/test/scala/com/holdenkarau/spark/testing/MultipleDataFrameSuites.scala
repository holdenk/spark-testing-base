package com.holdenkarau.spark.testing

import org.scalatest.funsuite.AnyFunSuite

class MultipleDataFrameSuites extends AnyFunSuite with DataFrameSuiteBase {
  test("test nothing") {
    assert(1 === 1)
  }
}
