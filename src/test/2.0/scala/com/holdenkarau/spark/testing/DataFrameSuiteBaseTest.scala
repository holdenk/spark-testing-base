package com.holdenkarau.spark.testing

import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StructField, StructType}
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

class DataFrameSuiteBaseTest extends FunSuite with SharedSparkContext with Checkers {

  test("testSchemaErrorMessage") {
    val expectedSchema = StructType(Seq(StructField("colA", IntegerType, false, new MetadataBuilder().putBoolean("someKey", false).build())))

    val resultSchema = StructType(Seq(StructField("colA", IntegerType, false, new MetadataBuilder().putBoolean("someKey", true).build())))

    val expectedErrorString = "Expected Schema: StructType(StructField(colA,IntegerType,false,{\"someKey\":false}) does not match result Schema: StructType(StructField(colA,IntegerType,false,{\"someKey\":true})"

    val resultErrorString = DataFrameSuiteBase.schemaErrorMessage(expectedSchema, resultSchema)

    assert(expectedErrorString.equals(resultErrorString))
  }
}
