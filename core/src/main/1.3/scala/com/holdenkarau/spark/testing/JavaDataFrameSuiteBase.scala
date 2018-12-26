package com.holdenkarau.spark.testing

class JavaDataFrameSuiteBase extends
    SharedJavaSparkContext with DataFrameSuiteBaseLike with JavaTestSuite {

  override def beforeAllTestCasesHook() {
    sqlBeforeAllTestCases()
  }

}
