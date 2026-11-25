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
import java.time.Duration

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.collection.JavaConverters._

case class Pandas(name: String, zip: String, pandaSize: Integer, age: Integer)

class SampleDataFrameTest extends ScalaDataFrameSuiteBase {
  val byteArray = new Array[Byte](1)
  val diffByteArray = Array[Byte](192.toByte)
  val inputList = List(
    Magic("panda", 9001.0, byteArray),
    Magic("coffee", 9002.0, byteArray))
  val inputList2 = List(
    Magic("panda", 9001.0 + 1E-6, byteArray),
    Magic("coffee", 9002.0, byteArray))

  test("assertColumnEquality") {
    import sqlContext.implicits._
    def myAddFunction(colName1: String, colName2: String): Column = {
      col(colName1) + col(colName2)
    }
    val df = spark.createDataFrame(
      List(
        Row(1, 3, 4),
        Row(5, 3, 8)
      ).asJava, StructType(List(
        StructField("num1", IntegerType, true),
        StructField("num2", IntegerType, true),
        StructField("expected", IntegerType, true)
      ))
    ).withColumn(
      "the_sum",
      myAddFunction("num1", "num2")
    )

    assertColumnEquality(df, "expected", "the_sum")
  }

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
    val row11 = Row("abc", 1.1, Timestamp.valueOf("2018-01-12 20:23:13"))
    val row11a = Row("abc", 1.2, Timestamp.valueOf("2018-01-12 20:23:15"))
    val row12 = Row(new java.math.BigDecimal(1.0))
    val row12a = Row(new java.math.BigDecimal(1.0 + 1.0E-6))
    val row13 = Row(scala.math.BigDecimal(1.0))
    val row13a = Row(scala.math.BigDecimal(1.0 + 1.0E-6))
    val row14 = Row(
      "abc", 1.1,
      Row("any", Row(Timestamp.valueOf("2018-01-12 20:23:13"))),
      "abc"
    )
    val row14a = Row(
      "abc", 1.2,
      Row("any", Row(Timestamp.valueOf("2018-01-12 20:23:15"))),
      "abc"
    )
    val row15 = Row(
      "some string",
      Row(true, "28/02/2024", Seq(Timestamp.valueOf("2018-01-12 20:23:13"))),
      Seq(
        Row("something", "anything", null, Row("row1"), Row(Seq("Apple"))),
        Row("", null, Row(Seq("email@xxxx.com"), "name", ""), Row("row2"), null)
      ),
      Seq(Row(
        Seq(1.1),
        Seq(1.1f),
        Seq(new java.math.BigDecimal(1.1)),
        Seq(scala.math.BigDecimal(1.1))
      ))
    )
    val row15a = Row(
      "some string",
      Row(true, "28/02/2024", Seq(Timestamp.valueOf("2018-01-12 20:23:15"))),
      Seq(
        Row("something", "anything", null, Row("row1"), Row(Seq("Apple"))),
        Row("", null, Row(Seq("email@xxxx.com"), "name", ""), Row("row2"), null)
      ),
      Seq(Row(
        Seq(1.2),
        Seq(1.2f),
        Seq(new java.math.BigDecimal(1.2)),
        Seq(scala.math.BigDecimal(1.2))
      ))
    )
    val row15b = Row(
      "some string",
      Row(true, "28/02/2024", Seq(Timestamp.valueOf("2018-01-12 20:23:13"))),
      Seq(
        Row("something", "anything", null, Row("row1"), Row(Seq("Apple"))),
        Row("", null, Row(Seq("AAAA@xxxx.com"), "name", ""), Row("row2"), null)
      ),
      Seq(Row(
        Seq(1.1),
        Seq(1.1f),
        Seq(new java.math.BigDecimal(1.1)),
        Seq(scala.math.BigDecimal(1.1))
      ))
    )
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
    assert(true === approxEquals(row11, row11a, 0.1, Duration.ofSeconds(5)))
    assert(true === approxEquals(row12, row12a, 1.0E-6))
    assert(true === approxEquals(row13, row13a, 1.0E-6))
    assert(true === approxEquals(row14, row14a, 0.1, Duration.ofSeconds(5)))
    assert(true === approxEquals(row15, row15a, 0.2, Duration.ofSeconds(3)))
    assert(false === approxEquals(row15, row15b, 0, Duration.ZERO))
  }

  test("verify hive function support") {
    import sqlContext.implicits._
    // Convert to int since old versions of hive only support percentile on
    // integer types.
    val intInputs = inputList.map(a => IntMagic(a.name, a.power.toInt, a.byteArray))
    val df = sc.parallelize(intInputs).toDF
    df.createOrReplaceTempView("pandaTemp")
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
    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDataFrameNoOrderEquals(input, input2)
    }
  }

  test("equal dataframes with nulls should be equal") {
    import sqlContext.implicits._
    val inputListWithNull = List(Magic(null, 9001.0, byteArray))
    val input = sc.parallelize(inputListWithNull).toDF
    assertDataFrameEquals(input, input)
    assertDataFrameApproximateEquals(input, input, 1E-5)
    assertDataFrameNoOrderEquals(input, input)
  }

  test("equal dataframes with different field order should be equal") {
    val df1 = rowDf(
      "a" -> (IntegerType, 1),
      "b" -> (StringType, "a string"))
    val df2 = rowDf(
      "b" -> (StringType, "a string"),
      "a" -> (IntegerType, 1))
    assertDataFrameDataEquals(df1, df2)
  }

  test("assertDataFrameDataEquals fails dataframes with different columns size") {
    val df1 = rowDf(
      "a" -> (IntegerType, 1),
      "b" -> (StringType, "a string"))
    val df2 = rowDf(
      "b" -> (StringType, "a string"),
      "a" -> (IntegerType, 1),
      "c" -> (IntegerType, 1))
    val thrown = intercept[Exception] {
      assertDataFrameDataEquals(df1, df2)
    }
    assert(thrown.getMessage contains "Column size not Equal")
  }

  test("assertSmallDataFrameDataEquals asserts DFs irrespective of fields order") {
    val df1 = rowDf(
      "a" -> (IntegerType, 1),
      "b" -> (StringType, "a"),
      "c" -> (IntegerType, 3))
    val df2 = rowDf(
      "b" -> (StringType, "a"),
      "a" -> (IntegerType, 1),
      "c" -> (IntegerType, 3))
    assertSmallDataFrameDataEquals(df1, df2)
  }

  test("assertSmallDataFrameDataEquals asserts DFs also validates schema") {
    val df1 = rowDf(
      "a" -> (IntegerType, 1),
      "b" -> (StringType, "a"),
      "c" -> (IntegerType, 3))
    val df2 = rowDf(
      "b" -> (StringType, "a"),
      "a" -> (IntegerType, 1),
      "c" -> (StringType, "3"))

    val thrown = intercept[Exception] {
      assertSmallDataFrameDataEquals(df1, df2)
    }
    assert(thrown.getMessage contains "false did not equal true")
  }

  test("assertSmallDataFrameDataEquals fails DFs with different columns size") {
    val df1 = rowDf(
      "a" -> (IntegerType, 1),
      "b" -> (StringType, "a"),
      "c" -> (IntegerType, 3))
    val df2 = rowDf(
      "b" -> (StringType, "a"),
      "a" -> (IntegerType, 1))

    val thrown = intercept[Exception] {
      assertSmallDataFrameDataEquals(df1, df2)
    }
    assert(thrown.getMessage contains "Column size not Equal")
  }

  testCombined("equal DF of rows of bytes should be equal (see GH issue #247)") {
    val df = rowDf(
      "a" -> (BinaryType, "bytes".getBytes()),
      "b" -> (StringType, "good"))
    assertDataFrameEquals(df, df)
  }

  def rowDf(fields: (String, (DataType, Any))*): DataFrame = {
    val row = Row(fields.map(_._2._2): _*)
    val rdd = sc.parallelize(List(row))
    val schema = StructType(fields.map(f => StructField(f._1, f._2._1)))
    sqlContext.createDataFrame(rdd, schema)
  }

  private def createDF(list: List[Row], fields: (String, DataType)*) =
    sqlContext.createDataFrame(sc.parallelize(list), structType2(fields))

  private def structType2(fields: Seq[(String, DataType)]) =
    StructType(fields.map(f => StructField(f._1, f._2)).toList)

  test("ignore magic schema fields") {
    val pandasList = List(Pandas("bata", "10010", 10, 2),
      Pandas("wiza", "10010", 20, 4),
      Pandas("dabdob", "11000", 8, 2),
      Pandas("hanafy", "11000", 15, 7),
      Pandas("hamdi", "11111", 20, 10))

    val inputDF = sqlContext.createDataFrame(pandasList)
    val resultDF = inputDF.groupBy(inputDF("zip")).agg(Map("pandaSize" -> "min", "age" -> "max"))

    val expectedRows = List(
      Row(pandasList(1).zip, pandasList(0).pandaSize, pandasList(1).age),
      Row(pandasList(3).zip, pandasList(2).pandaSize, pandasList(3).age),
      Row(pandasList(4).zip, pandasList(4).pandaSize, pandasList(4).age))

    val expectedDF = createDF(expectedRows, ("zip", StringType),
                                            ("min(pandaSize)", IntegerType),
                                            ("max(age)", IntegerType))

    assertDataFrameEquals(expectedDF.orderBy("zip"), resultDF.orderBy("zip"))
  }

  test("convertMapToArrayStruct should return a schema with struct type instead of a map"){

    import sqlContext.implicits._
    val originalDF = Seq(
      BigTestClass(
        simpleMap = Map("one" -> 1, "two" -> 2),
        arrayOfStruct = Array(
          InnerStruct("x", 10, Map("one" -> 1)),
          InnerStruct("y", 20, Map("one" -> 1))),
        nestedStruct = DeepStruct(
        nested = InnerStruct("deep", 9, Map("one" -> 1)),
        numbers = Array(1, 2, 3),
        meta = Map("k1" -> "v1")
        ),
        arrayMap = Array(Map("one" -> 1), Map("two" -> 2))
      )
    ).toDF()

    val resultDF = convertMapToArrayStruct(originalDF)

   
    val expectedSchema = StructType(Seq(
    StructField(
      "simpleMap",
      ArrayType(
        StructType(Seq(
          StructField("0", StringType, nullable = true),
          StructField("1", IntegerType, nullable = true)
        )),
        containsNull = false
      ),
      nullable = true
    ),
    StructField(
      "arrayOfStruct",
      ArrayType(
        StructType(Seq(
          StructField("a", StringType, nullable = true),
          StructField("b", IntegerType, nullable = true),
          StructField(
            "c",
            ArrayType(
              StructType(Seq(
                StructField("0", StringType, nullable = true),
                StructField("1", IntegerType, nullable = true)
              )),
              containsNull = false
            ),
            nullable = true
          )
        )),
        containsNull = false
      ),
      nullable = true
    ),
    StructField(
      "nestedStruct",
      StructType(Seq(
        StructField(
          "nested",
          StructType(Seq(
            StructField("a", StringType, nullable = true),
            StructField("b", IntegerType, nullable = true),
            StructField(
              "c",
              ArrayType(
                StructType(Seq(
                  StructField("0", StringType, nullable = true),
                  StructField("1", IntegerType, nullable = true)
                )),
                containsNull = false
              ),
              nullable = true
            )
          )),
          nullable = false
        ),
        StructField(
          "numbers",
          ArrayType(IntegerType, containsNull = false),
          nullable = true
        ),
        StructField(
          "meta",
          ArrayType(
            StructType(Seq(
              StructField("0", StringType, nullable = true),
              StructField("1", StringType, nullable = true)
            )),
            containsNull = false
          ),
          nullable = true
        )
      )),
      nullable = false
    ),
    StructField(
      "arrayMap",
      ArrayType(
        StructType(Seq(
          StructField("0", StringType, nullable = true),
          StructField("1", IntegerType, nullable = true)
        )),
        containsNull = false
      ),
      nullable = true
      )
    ))
   
    assert(expectedSchema == resultDF.schema)

  }

  test("assertDataFrameDataEquals should work with map types converting it to structs") {

    import sqlContext.implicits._
    val originalDF = Seq(
      BigTestClass(
        simpleMap = Map("one" -> 1, "two" -> 2),
        arrayOfStruct = Array(
          InnerStruct("x", 10, Map("one" -> 1)),
          InnerStruct("y", 20, Map("one" -> 1))),
        nestedStruct = DeepStruct(
        nested = InnerStruct("deep", 9, Map("one" -> 1)),
        numbers = Array(1, 2, 3),
        meta = Map("k1" -> "v1")
        ),
        arrayMap = Array(Map("one" -> 1), Map("two" -> 2))
      )
    ).toDF()

    //Same dataframes should be equal
    assertDataFrameDataEquals(originalDF, originalDF)

    //Part 2: test that it correctly detects different dataframes
    val modifiedDF = Seq(
      BigTestClass(
        simpleMap = Map("one" -> 999, "two" -> 2), // Here comes the difference
        arrayOfStruct = Array(
          InnerStruct("x", 10, Map("one" -> 1)),
          InnerStruct("y", 20, Map("one" -> 1))),
        nestedStruct = DeepStruct(
        nested = InnerStruct("deep", 9, Map("one" -> 1)),
        numbers = Array(1, 2, 3),
        meta = Map("k1" -> "v1")
        ),
        arrayMap = Array(Map("one" -> 1), Map("two" -> 2))
      )
    ).toDF()

    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDataFrameDataEquals(originalDF, modifiedDF)
    }
  }


  private def failureMessage(f: => Unit): String =
    intercept[org.scalatest.exceptions.TestFailedException](f).getMessage

  private def doubleDf(values: java.lang.Double*): DataFrame =
    createDF(values.map(v => Row(v)).toList, ("d", DoubleType))

  test("NaN and infinities are only equal to themselves (SPARK-59514)") {
    val NaN = Double.NaN
    val Inf = Double.PositiveInfinity
    val unequal = Seq((NaN, 1.0), (NaN, Inf), (1.0, Inf), (1.0, -Inf), (Inf, -Inf))
    for ((a, b) <- unequal; (x, y) <- Seq((a, b), (b, a));
         options <- Seq(DataFrameEqualityOptions(),
                        DataFrameEqualityOptions(tol = 10.0, rtol = 0.5),
                        DataFrameEqualityOptions(tol = 10.0, checkRowOrder = false))) {
      intercept[org.scalatest.exceptions.TestFailedException] {
        assertDataFrameEquals(doubleDf(x), doubleDf(y), options)
      }
    }
    for (v <- Seq(NaN, Inf, -Inf)) {
      assertDataFrameEquals(doubleDf(v), doubleDf(v))
      assertDataFrameApproximateEquals(doubleDf(v), doubleDf(v), 0.1, Duration.ZERO)
      assertDataFrameNoOrderEquals(doubleDf(v), doubleDf(v))
    }
  }

  test("arrays from real DataFrames get tolerance (not just Lists)") {
    import sqlContext.implicits._
    val expected = Seq((1, Seq(1.0, 2.0))).toDF("id", "values")
    val close = Seq((1, Seq(1.05, 2.05))).toDF("id", "values")
    val far = Seq((1, Seq(1.5, 2.0))).toDF("id", "values")
    val longer = Seq((1, Seq(1.0, 2.0, 3.0))).toDF("id", "values")
    assertDataFrameApproximateEquals(expected, close, 0.1, Duration.ZERO)
    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDataFrameApproximateEquals(expected, far, 0.1, Duration.ZERO)
    }
    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDataFrameApproximateEquals(expected, longer, 10.0, Duration.ZERO)
    }
    val withNaN = Seq((1, Seq(1.0, Double.NaN))).toDF("id", "values")
    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDataFrameApproximateEquals(withNaN, close, 10.0, Duration.ZERO)
    }
    assertDataFrameApproximateEquals(withNaN, withNaN, 0.0, Duration.ZERO)
  }

  test("maps and nested structs get tolerance") {
    import sqlContext.implicits._
    val expected = Seq((Map("a" -> 1.0), (1.0, "x"))).toDF("m", "s")
    val close = Seq((Map("a" -> 1.05), (1.05, "x"))).toDF("m", "s")
    assertDataFrameEquals(expected, close, DataFrameEqualityOptions(tol = 0.1))
    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDataFrameEquals(expected, close)
    }
  }

  test("relative tolerance") {
    val options = DataFrameEqualityOptions(rtol = 0.01)
    assertDataFrameEquals(doubleDf(1000.0), doubleDf(1009.0), options)
    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDataFrameEquals(doubleDf(1000.0), doubleDf(1011.0), options)
    }
  }

  test("unordered comparison with and without tolerance") {
    val expected = doubleDf(1.0, 2.0, Double.NaN, null)
    val shuffled = doubleDf(null, Double.NaN, 2.0, 1.0)
    val shuffledClose = doubleDf(null, Double.NaN, 2.01, 1.01)
    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDataFrameEquals(expected, shuffled)
    }
    assertDataFrameEquals(expected, shuffled, DataFrameEqualityOptions(checkRowOrder = false))
    assertDataFrameEquals(expected, shuffledClose,
      DataFrameEqualityOptions(checkRowOrder = false, tol = 0.1))
    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDataFrameEquals(expected, shuffledClose,
        DataFrameEqualityOptions(checkRowOrder = false))
    }
  }

  test("ignoreNullable ignores nested nullability") {
    val schema = (nullable: Boolean) => StructType(Seq(
      StructField("a", ArrayType(DoubleType, containsNull = nullable), nullable = nullable)))
    val rows = sc.parallelize(List(Row(Seq(1.0))))
    val nullable = sqlContext.createDataFrame(rows, schema(true))
    val notNullable = sqlContext.createDataFrame(rows, schema(false))
    val message = failureMessage(assertDataFrameEquals(nullable, notNullable))
    assert(message.contains("Schemas differ"))
    assertDataFrameEquals(nullable, notNullable, DataFrameEqualityOptions(ignoreNullable = true))
  }

  test("ignoreColumnOrder, ignoreColumnName and ignoreColumnType") {
    val expected = rowDf("a.b" -> (IntegerType, 1), "c" -> (StringType, "x"))
    val reordered = rowDf("c" -> (StringType, "x"), "a.b" -> (IntegerType, 1))
    val renamed = rowDf("x" -> (IntegerType, 1), "y" -> (StringType, "x"))
    val retyped = rowDf("a.b" -> (DoubleType, 1.0), "c" -> (StringType, "x"))
    val cases = Seq(
      reordered -> DataFrameEqualityOptions(ignoreColumnOrder = true),
      renamed -> DataFrameEqualityOptions(ignoreColumnName = true),
      retyped -> DataFrameEqualityOptions(ignoreColumnType = true))
    for ((result, options) <- cases) {
      intercept[org.scalatest.exceptions.TestFailedException] {
        assertDataFrameEquals(expected, result)
      }
      assertDataFrameEquals(expected, result, options)
      assertDataFrameEquals(expected, result, options.withCheckRowOrder(false))
    }
  }

  test("failure message shows counts, differing rows and respects maxErrors") {
    import sqlContext.implicits._
    val expected = (1 to 20).map(i => (i, "same")).toDF("i", "s")
    val result = (1 to 20).map(i => (i + 1, "same")).toDF("i", "s")
    val message = failureMessage(assertDataFrameEquals(expected, result,
      DataFrameEqualityOptions(maxErrors = Some(2)), (_: DataFrame) => ()))
    assert(message.contains("There are some unequal rows"))
    assert(message.contains("20 of 20 rows differ"))
    assert(message.contains("showing the first 2"))
    assert(message.contains("expected: [!1,same]"))
    val onlyDiff = failureMessage(assertDataFrameEquals(expected, result,
      DataFrameEqualityOptions(maxErrors = Some(1), showOnlyDiff = true)))
    assert(onlyDiff.contains("! i: expected=1, actual=2"))
    assert(!onlyDiff.contains("same"))
    val lengths = failureMessage(assertDataFrameEquals(expected, expected.limit(3)))
    assert(lengths.contains("expected 20 rows, result has 3 rows"))
  }

  test("unordered failure message lists the differing rows") {
    import sqlContext.implicits._
    val expected = Seq(("a", 1), ("b", 2), ("b", 2)).toDF("k", "v")
    val result = Seq(("a", 1), ("b", 2), ("c", 3)).toDF("k", "v")
    val message = failureMessage(assertDataFrameNoOrderEquals(expected, result))
    assert(message.contains("[k=b, v=2]: expected 2 time(s), found 1 time(s)"))
    assert(message.contains("[k=c, v=3]: expected 0 time(s), found 1 time(s)"))
  }

  test("customShow still receives the tagged rows") {
    import sqlContext.implicits._
    var shown: Option[Array[Row]] = None
    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDataFrameEquals(Seq(1).toDF("v"), Seq(2).toDF("v"),
        (df: DataFrame) => shown = Some(df.collect()))
    }
    assert(shown.get.toSeq === Seq(Row("expected", 1), Row("result", 2)))
  }

  test("assertColumnEquality catches nulls on only one side") {
    val df = createDF(List(Row(1, 1), Row(null, null), Row(2, null)),
      ("a", IntegerType), ("b", IntegerType))
    intercept[ColumnMismatch] {
      assertColumnEquality(df, "a", "b")
    }
    assertColumnEquality(df.filter("a is null or a = 1"), "a", "b")
  }

}


case class Magic(name: String, power: Double, byteArray: Array[Byte])
case class IntMagic(name: String, power: Int, byteArray: Array[Byte])

case class InnerStruct(a: String, b: Int, c: Map[String, Int])
case class DeepStruct(
    nested: InnerStruct,
    numbers: Array[Int],
    meta: Map[String, String]
  )

case class BigTestClass(
    simpleMap: Map[String, Int],
    arrayOfStruct: Array[InnerStruct],
    nestedStruct: DeepStruct,
    arrayMap : Array[Map[String, Int]]
  )

