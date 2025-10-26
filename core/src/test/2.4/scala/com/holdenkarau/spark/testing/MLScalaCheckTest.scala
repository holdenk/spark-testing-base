package com.holdenkarau.spark.testing

import org.apache.spark.ml.linalg.SQLDataTypes.{MatrixType, VectorType}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalacheck.Prop.forAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.Checkers
import org.apache.spark.sql.SparkSession

class MLScalaCheckTest extends AnyFunSuite with SharedSparkContext with Checkers {
  // re-use the spark context
  override implicit def reuseContextIfPossible: Boolean = false

  test("vector generation") {
    val schema = StructType(List(StructField("vector", VectorType)))
    val sqlContext = SparkSession.builder.getOrCreate().sqlContext
    val dataFrameGen = DataFrameGenerator.arbitraryDataFrame(sqlContext, schema)

    val property =
      forAll(dataFrameGen.arbitrary) {
        dataFrame => {
          dataFrame.schema === schema && dataFrame.count >= 0
        }
      }

    check(property)
  }

  test("matrix generation") {
    val schema = StructType(List(StructField("matrix", MatrixType)))
    val sqlContext = SparkSession.builder.getOrCreate().sqlContext
    val dataFrameGen = DataFrameGenerator.arbitraryDataFrame(sqlContext, schema)

    val property =
      forAll(dataFrameGen.arbitrary) {
        dataFrame => {
          dataFrame.schema === schema && dataFrame.count >= 0
        }
      }

    check(property)
  }
}
