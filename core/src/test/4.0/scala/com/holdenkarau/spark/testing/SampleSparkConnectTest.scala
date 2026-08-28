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

/**
 * Demonstrates that adding `with ConnectEnabled` to an existing
 * ScalaDataFrameSuiteBase test makes everything go through Spark Connect --
 * same assertion methods, same `spark` session, just routed over gRPC.
 */
class SampleSparkConnectTest extends ScalaDataFrameSuiteBase
    with ConnectEnabled {

  // Proof that the session the assertions use really is the Connect one. If
  // this ever silently falls back to a classic session, the rest of this suite
  // would still pass while testing nothing, so keep it first.
  test("the suite's session is the Connect session") {
    assert(isConnectSession)
    assert(spark eq SparkSessionProvider._sparkSession)
    assert(spark.getClass.getName.contains(".connect."),
      s"expected a Connect SparkSession, got ${spark.getClass.getName}")
  }

  test("classic escape hatches are unavailable over Connect") {
    import spark.implicits._
    val df = Seq(1, 2, 3).toDF("value")
    // .rdd does not exist over Connect -- if this doesn't throw we aren't
    // actually going through Connect.
    intercept[Exception] {
      df.rdd.count()
    }
    intercept[Exception] {
      spark.sparkContext
    }
  }

  test("create and query a DataFrame through Connect") {
    import spark.implicits._

    val df = Seq(("Alice", 30), ("Bob", 25), ("Charlie", 35))
      .toDF("name", "age")

    assert(df.count() === 3)
    assert(df.columns.toSeq === Seq("name", "age"))
  }

  test("SQL queries work through Connect") {
    import spark.implicits._

    val df = Seq(("Alice", 30), ("Bob", 25))
      .toDF("name", "age")
    df.createOrReplaceTempView("people")

    val result = spark.sql("SELECT name FROM people WHERE age > 26")
    val names = result.collect().map(_.getString(0))
    assert(names.toSet === Set("Alice"))
  }

  test("dataframe should be equal to itself") {
    import spark.implicits._
    val df = Seq(1, 2, 3).toDF("value")
    assertDataFrameEquals(df, df)
  }

  test("unequal dataframes should not be equal") {
    import spark.implicits._
    val df1 = Seq(1, 2, 3).toDF("value")
    val df2 = Seq(1, 2, 99).toDF("value")
    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDataFrameEquals(df1, df2)
    }
  }

  test("dataframe should be equal with different order of rows") {
    import spark.implicits._
    val df1 = Seq(("a", 1), ("b", 2), ("c", 3)).toDF("key", "value")
    val df2 = Seq(("c", 3), ("a", 1), ("b", 2)).toDF("key", "value")
    assertDataFrameNoOrderEquals(df1, df2)
  }

  test("unequal dataframe with different order should not equal") {
    import spark.implicits._
    val df1 = Seq(("a", 1), ("b", 2)).toDF("key", "value")
    val df2 = Seq(("a", 1), ("c", 3)).toDF("key", "value")
    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDataFrameNoOrderEquals(df1, df2)
    }
  }

  test("dataframe approx expected") {
    import spark.implicits._
    val df1 = Seq(1.0, 2.0, 3.0).toDF("value")
    val df2 = Seq(1.001, 2.001, 3.001).toDF("value")
    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDataFrameApproximateEquals(df1, df2, 1E-5)
    }
    assertDataFrameApproximateEquals(df1, df2, 0.01)
  }

  test("empty dataframes should be equal") {
    import spark.implicits._
    val empty1 = Seq.empty[Int].toDF("value")
    val empty2 = Seq.empty[Int].toDF("value")
    assertDataFrameEquals(empty1, empty2)
    assertDataFrameNoOrderEquals(empty1, empty2)
  }

  test("unequal length dataframes should not be equal") {
    import spark.implicits._
    val df1 = Seq(1, 2, 3).toDF("value")
    val df2 = Seq(1, 2).toDF("value")
    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDataFrameEquals(df1, df2)
    }
    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDataFrameNoOrderEquals(df1, df2)
    }
  }

  test("datasets should compare over Connect") {
    import spark.implicits._
    val ds1 = Seq(1, 2, 3).toDS
    val ds2 = Seq(1, 2, 99).toDS
    assertDatasetEquals(ds1, ds1)
    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDatasetEquals(ds1, ds2)
    }
  }
}
