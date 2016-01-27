package com.holdenkarau.spark.testing

import org.apache.spark.sql.types.StructType
import org.junit.Assert.assertEquals

class JavaDataFrameSuiteBase extends JavaSuiteBase with DataFrameSuiteBaseLike {
  // Version of equalSchema using JUNit assertEquals
  override def equalSchema(expected: StructType, result: StructType): Unit = {
    assertEquals(expected.treeString, result.treeString)
  }

  override def beforeAllTestCasesHook() {
    sqlBeforeAllTestCases()
  }
}
