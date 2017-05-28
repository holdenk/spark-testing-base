package com.holdenkarau.spark.testing

import com.holdenkarau.spark.testing.DataFrameSuiteBase.schemaErrorMessage
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

import scala.collection.JavaConversions._


case class PersonAge(name: String, age: Int)

class DataFrameSuiteBaseTest
extends FunSuite
with SharedSparkContext
with Checkers
with DataFrameSuiteBase {

  import sqlContext.implicits._

  test("testSchemaErrorMessage") {
    val metadataOne =
      new MetadataBuilder().putBoolean("someKey", false).build()
    val expectedField =
      Seq(StructField("colA", IntegerType, false, metadataOne))
    val expectedSchema = StructType(expectedField)

    val metadataTwo = new MetadataBuilder().putBoolean("someKey", true).build()
    val resultField = Seq(StructField("colA", IntegerType, false, metadataTwo))
    val resultSchema = StructType(resultField)

    val errorString = """
      |Expected Schema: StructType(StructField(colA,IntegerType,false,{"someKey":false})
      |does not match
      |Result Schema: StructType(StructField(colA,IntegerType,false,{"someKey":true})
    """.stripMargin

    val resultErrorString =
      schemaErrorMessage(expectedSchema, resultSchema)

    assert(errorString.equals(resultErrorString))
  }

  test("assertSchemaEquals passes when schema are equal") {

    val data = List(PersonAge("Alice", 12), PersonAge("Bob", 45))
    val expectedDf = sc.parallelize(data).toDF
    val resultDf = sc.parallelize(data).toDF

    assertDataFrameEquals(expectedDf, resultDf)
  }

  test("assertSchemaEquals fails with proper message when schema differ") {
    val data = List(Row("Alice"))
    val expectedMetadata =
      new MetadataBuilder().putBoolean("status", true).build()
    val expectedField = StructField("name", StringType, true, expectedMetadata)
    val expectedSchema = StructType(Array(expectedField))
    val resultMetadata =
      new MetadataBuilder().putBoolean("status", false).build()
    val resultField = StructField("name", StringType, true, resultMetadata)
    val resultSchema = StructType(Array(resultField))
    val expectedDf = sqlContext.createDataFrame(data, expectedSchema)
    val resultDf = sqlContext.createDataFrame(data, resultSchema)
    val failedException =
      intercept[org.scalatest.exceptions.TestFailedException] {
        schemaEquals(expectedDf, resultDf)
      }

    val assertErrorString = "StructType(StructField(name,StringType,true))" +
      " did not equal " +
      "StructType(StructField(name,StringType,true))"
    val customErrorString = schemaErrorMessage(expectedSchema, resultSchema)
    val errorString =  assertErrorString + customErrorString
    assert(failedException.getMessage() == errorString)
  }
}
