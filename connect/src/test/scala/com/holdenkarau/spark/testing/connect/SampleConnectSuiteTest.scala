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

package com.holdenkarau.spark.testing.connect

/**
 * The Spark 3.5 Connect lane: no spark-sql on this classpath at all, so
 * `spark` can only be a Connect session and every assertion below necessarily
 * runs over gRPC.
 */
class SampleConnectSuiteTest extends ScalaConnectSuiteBase {

  test("this really is a Connect-only classpath") {
    // spark-sql is not a dependency of this project, so a class that only
    // spark-sql ships must not be loadable. If it ever becomes loadable the
    // lane has been compromised and these tests would no longer prove anything.
    //
    // QueryExecution specifically: it is in spark-sql 3.5 and absent from
    // spark-connect-client-jvm 3.5. org.apache.spark.sql.classic.SparkSession
    // would be the obvious probe but that package only exists in Spark 4, so
    // on 3.5 it is absent from both jars and the assertion would hold whether
    // or not spark-sql had leaked in.
    val sqlOnly = "org.apache.spark.sql.execution.QueryExecution"
    intercept[ClassNotFoundException] {
      Class.forName(sqlOnly, false, getClass.getClassLoader)
    }
    assert(connectRemote.startsWith("sc://"))
  }

  test("create and query a DataFrame over Connect") {
    import spark.implicits._
    val df = Seq(("Alice", 30), ("Bob", 25), ("Charlie", 35)).toDF("name", "age")
    assert(df.count() === 3)
    assert(df.columns.toSeq === Seq("name", "age"))
  }

  test("SQL queries work over Connect") {
    import spark.implicits._
    Seq(("Alice", 30), ("Bob", 25)).toDF("name", "age")
      .createOrReplaceTempView("people")
    val names = spark.sql("SELECT name FROM people WHERE age > 26")
      .collect().map(_.getString(0))
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

  test("assertColumnEquality works over Connect") {
    import spark.implicits._
    val df = Seq((1, 1), (2, 2)).toDF("a", "b")
    assertColumnEquality(df, "a", "b")
    val bad = Seq((1, 1), (2, 3)).toDF("a", "b")
    intercept[com.holdenkarau.spark.testing.ColumnMismatch] {
      assertColumnEquality(bad, "a", "b")
    }
  }
}
