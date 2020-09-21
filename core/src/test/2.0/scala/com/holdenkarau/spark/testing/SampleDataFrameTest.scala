/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.holdenkarau.spark.testing

import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

class SampleDataFrameTest extends FunSuite with DataFrameSuiteBase {
  val byteArray = new Array[Byte](1)
  val diffByteArray = Array[Byte](192.toByte)
  val inputList = List(
    Magic("panda", 9001.0, byteArray),
    Magic("coffee", 9002.0, byteArray))
  val inputList2 = List(
    Magic("panda", 9001.0 + 1E-6, byteArray),
    Magic("coffee", 9002.0, byteArray))

  test("dataframe should be equal to its self") {
    import sqlContext.implicits._
    val input = sc.parallelize(inputList).toDF
    assertDataFrameEquals(input, input)
  }

  test("dataframe should be equal with different order of rows") {
    import sqlContext.implicits._
    val inputListWithDuplicates = inputList ++ List(inputList.head)
    val input = sc.parallelize(inputListWithDuplicates).toDF
    val reverseInput = sc.parallelize(inputListWithDuplicates.reverse).toDF
    assertDataFrameNoOrderEquals(input, reverseInput)
  }

  test("empty dataframes should be equal") {
    import sqlContext.implicits._
    val emptyList = spark.emptyDataset[Magic].toDF()
    val emptyList2 = spark.emptyDataset[Magic].toDF()
    assertDataFrameEquals(emptyList, emptyList2)
    assertDataFrameNoOrderEquals(emptyList, emptyList2)
  }

  test("empty dataframes should be not be equal to nonempty ones") {
    import sqlContext.implicits._
    val emptyList = spark.emptyDataset[Magic].toDF()
    val input = sc.parallelize(inputList).toDF
    assertThrows[org.scalatest.exceptions.TestFailedException] {
      assertDataFrameEquals(emptyList, input)
    }
    assertThrows[org.scalatest.exceptions.TestFailedException] {
      assertDataFrameNoOrderEquals(emptyList, input)
    }
    assertThrows[org.scalatest.exceptions.TestFailedException] {
      assertDataFrameEquals(input, emptyList)
    }
    assertThrows[org.scalatest.exceptions.TestFailedException] {
      assertDataFrameNoOrderEquals(input, emptyList)
    }
  }

  test("unequal dataframes should not be equal") {
    import sqlContext.implicits._
    val input = sc.parallelize(inputList).toDF
    val input2 = sc.parallelize(inputList2).toDF
    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDataFrameEquals(input, input2)
    }
  }

  test("unequal dataframe with different order should not equal") {
    import sqlContext.implicits._
    val inputListWithDuplicates = inputList ++ List(inputList.head)
    val input = sc.parallelize(inputListWithDuplicates).toDF
    val input2 = sc.parallelize(inputList).toDF
    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDataFrameNoOrderEquals(input, input2)
    }
    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDataFrameNoOrderEquals(input2, input)
    }
  }

  test("dataframe approx expected") {
    import sqlContext.implicits._
    val input = sc.parallelize(inputList).toDF
    val input2 = sc.parallelize(inputList2).toDF
    assertDataFrameApproximateEquals(input, input2, 1E-5)
    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDataFrameApproximateEquals(input, input2, 1E-7)
    }
  }

  test("dataframe approxEquals on rows") {
    import sqlContext.implicits._
    val row = sc.parallelize(inputList).toDF.collect()(0)
    val row2 = sc.parallelize(inputList2).toDF.collect()(0)
    val row3 = Row()
    val row4 = Row(1)
    val row5 = Row(null)
    val row6 = Row("1")
    val row6a = Row("2")
    val row7 = Row(1.toFloat)
    val row8 = Row(Timestamp.valueOf("2018-01-12 20:22:13"))
    val row9 = Row(Timestamp.valueOf("2018-01-12 20:22:18"))
    val row10 = Row(Timestamp.valueOf("2018-01-12 20:23:13"))
    assert(false === approxEquals(row, row2, 1E-7))
    assert(true === approxEquals(row, row2, 1E-5))
    assert(true === approxEquals(row3, row3, 1E-5))
    assert(false === approxEquals(row, row3, 1E-5))
    assert(false === approxEquals(row4, row5, 1E-5))
    assert(true === approxEquals(row5, row5, 1E-5))
    assert(false === approxEquals(row4, row6, 1E-5))
    assert(false === approxEquals(row6, row4, 1E-5))
    assert(false === approxEquals(row6, row7, 1E-5))
    assert(false === approxEquals(row6, row6a, 1E-5))
    assert(true === approxEquals(row8, row9, 5000))
    assert(false === approxEquals(row9, row8, 3000))
    assert(true === approxEquals(row9, row10, 60000))
    assert(false === approxEquals(row9, row10, 53000))
  }

  test("verify hive function support") {
    import sqlContext.implicits._
    // Convert to int since old versions of hive only support percentile on
    // integer types.
    val intInputs = inputList.map(a => IntMagic(a.name, a.power.toInt, a.byteArray))
    val df = sc.parallelize(intInputs).toDF
    df.registerTempTable("pandaTemp")
    val df2 = sqlContext.sql(
      "select percentile(power, 0.5) from pandaTemp group by name")
    val result = df2.collect()
  }

  test("unequal dataframes should not be equal when length differs") {
    import sqlContext.implicits._
    val input = sc.parallelize(inputList).toDF
    val input2 = sc.parallelize(inputList.headOption.toSeq).toDF
    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDataFrameEquals(input, input2)
    }
    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDataFrameApproximateEquals(input, input2, 1E-5)
    }
  }

  test("unequal dataframes should not be equal when byte array differs") {
    import sqlContext.implicits._
    val input = sc.parallelize(inputList).toDF
    val diffInputList = List(Magic("panda", 9001.0, byteArray),
      Magic("coffee", 9002.0, diffByteArray))
    val input2 = sc.parallelize(diffInputList).toDF
    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDataFrameEquals(input, input2)
    }
    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDataFrameApproximateEquals(input, input2, 1E-5)
    }
  }

  test("equal DF of rows of bytes should be equal (see GH issue #247)") {
    import sqlContext.implicits._
    val row1 = Row("bytes".getBytes(), "good")
    val rdd = sc.parallelize(List(row1))
    val schema = StructType(
      List(StructField("a", BinaryType, true),
        StructField("b", StringType, true)))
    val df = sqlContext.createDataFrame(rdd, schema)
    assertDataFrameEquals(df, df)
  }
}

case class Magic(name: String, power: Double, byteArray: Array[Byte])
case class IntMagic(name: String, power: Int, byteArray: Array[Byte])
