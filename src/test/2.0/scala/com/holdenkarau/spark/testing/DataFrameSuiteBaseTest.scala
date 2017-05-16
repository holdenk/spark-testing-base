package com.holdenkarau.spark.testing

import org.apache.spark.sql.types.{
  IntegerType,
  MetadataBuilder,
  StructField,
  StructType
}
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

class DataFrameSuiteBaseTest
    extends FunSuite
    with SharedSparkContext
    with Checkers {

  test("testSchemaErrorMessage") {
    val metadataOne =
      new MetadataBuilder().putBoolean("someKey", false).build()
    val expectedField =
      Seq(StructField("colA", IntegerType, false, metadataOne))
    val expectedSchema = StructType(expectedField)

    val metadataTwo = new MetadataBuilder().putBoolean("someKey", true).build()
    val resultField = Seq(StructField("colA", IntegerType, false, metadataTwo))
    val resultSchema = StructType(resultField)

    //noinspection ScalaStyle
    val expectedErrorString =
      "Expected Schema: StructType(StructField(colA,IntegerType,false,{\"someKey\":false}) does not match result Schema: StructType(StructField(colA,IntegerType,false,{\"someKey\":true})"

    val resultErrorString =
      DataFrameSuiteBase.schemaErrorMessage(expectedSchema, resultSchema)

    assert(expectedErrorString.equals(resultErrorString))
  }
}
