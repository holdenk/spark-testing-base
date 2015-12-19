package com.holdenkarau.spark.testing

class JavaDataFrameSuiteBase extends SharedJavaSparkContext with DataFrameSuiteBaseLike {

  override def runBefore() {
    super.runBefore()

    if (!initialized)
      super.beforeAllTestCases()
  }
}
