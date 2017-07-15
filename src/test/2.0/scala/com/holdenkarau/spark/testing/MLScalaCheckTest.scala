package com.holdenkarau.spark.testing

import org.apache.spark.ml.linalg.SQLDataTypes.{MatrixType, VectorType}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalacheck.Prop.forAll
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

class MLScalaCheckTest extends FunSuite with SharedSparkContext with Checkers {
  // re-use the spark context
  override implicit def reuseContextIfPossible: Boolean = false

  test("vector generation") {
    val schema = StructType(List(StructField("vector", VectorType)))
    val sqlContext = new SQLContext(sc)
    val dataframeGen = DataframeGenerator.arbitraryDataFrame(sqlContext, schema)

    val property =
      forAll(dataframeGen.arbitrary) {
        dataframe => {
          dataframe.schema === schema && dataframe.count >= 0
        }
      }

    check(property)
  }

  test("matrix generation") {
    val schema = StructType(List(StructField("matrix", MatrixType)))
    val sqlContext = new SQLContext(sc)
    val dataframeGen = DataframeGenerator.arbitraryDataFrame(sqlContext, schema)

    val property =
      forAll(dataframeGen.arbitrary) {
        dataframe => {
          dataframe.schema === schema && dataframe.count >= 0
        }
      }

    check(property)
  }
}
