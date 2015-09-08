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

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import org.scalatest.FunSuite
import org.scalatest.exceptions.TestFailedException

class SampleDataFrameTest extends FunSuite with SharedSparkContext with DataFrameSuiteBase {
  val inputList = List(Magic("panda", 9001.0), Magic("coffee", 9002.0))
  val inputList2 = List(Magic("panda", 9001.0 + 1E-6), Magic("coffee", 9002.0))

  test("dataframe should be equal to its self") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val input = sc.parallelize(inputList).toDF
    equalDataFrames(input, input)
  }

  test("unequal dataframes should not be equal") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val input = sc.parallelize(inputList).toDF
    val input2 = sc.parallelize(inputList2).toDF
    intercept[org.scalatest.exceptions.TestFailedException] {
      equalDataFrames(input, input2)
    }
  }

  test("dataframe approx expected") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val input = sc.parallelize(inputList).toDF
    val input2 = sc.parallelize(inputList2).toDF
    approxEqualDataFrames(input, input2, 1E-5)
    intercept[org.scalatest.exceptions.TestFailedException] {
      approxEqualDataFrames(input, input2, 1E-7)
    }
  }
}

case class Magic(name: String, power: Double)
