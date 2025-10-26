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

import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming._

import org.scalatest.Suite

import org.scalatest.Assertion

/**
 * Early Experimental Structured Streaming Base.
 */
trait StructuredStreamingBase extends DataFrameSuiteBase
    with StructuredStreamingBaseLike { self: Suite =>
  /**
   * Test a simple streams end state
   */
  def testSimpleStreamEndState[T: Encoder, R: Encoder](
    spark: SparkSession,
    input: Seq[Seq[T]],
    expected: Seq[R],
    mode: String,
    queryFunction: Dataset[T] => Dataset[R]): Assertion = {
    val result = runSimpleStreamEndState(spark, input, mode, queryFunction)
    assert(result === expected)
  }
}

trait StructuredStreamingBaseLike extends SparkContextProvider
    with TestSuiteLike with Serializable {
  var count = 0
  /**
   * Run a simple streams end state
   */
  private[holdenkarau] def runSimpleStreamEndState[T: Encoder, R: Encoder](
    spark: SparkSession,
    input: Seq[Seq[T]],
    mode: String,
    queryFunction: Dataset[T] => Dataset[R]) = {
    import spark.implicits._
    implicit val sqlContext = spark.sqlContext
    val inputStream = MemoryStream[T]
    val transformed = queryFunction(inputStream.toDS())
    val queryName = s"${this.getClass.getSimpleName}TestSimpleStreamEndState${count}"
    count = count + 1
    val query = transformed.writeStream.
      format("memory").
      outputMode(mode).
      queryName(queryName).
      start()
    input.foreach(batch => inputStream.addData(batch))
    // Block until all processed
    query.processAllAvailable()
    val table = spark.table(queryName).as[R]
    val resultRows = table.collect()
    resultRows.toSeq
  }
}
