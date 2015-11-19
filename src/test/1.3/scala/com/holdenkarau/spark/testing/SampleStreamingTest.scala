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

import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import org.scalatest.FunSuite
import org.scalatest.exceptions.TestFailedException

class SampleStreamingTest extends StreamingSuiteBase {

  //tag::simpleStreamingTest[]
  test("really simple transformation") {
    val input = List(List("hi"), List("hi holden"), List("bye"))
    val expected = List(List("hi"), List("hi", "holden"), List("bye"))
    testOperation[String, String](input, tokenize _, expected, useSet = true)
  }

  // This is the sample function we are testing
  def tokenize(f: DStream[String]): DStream[String] = {
    f.flatMap(_.split(" "))
  }
  //end::simpleStreamingTest[]

  test("simple two stream streaming test") {
    val input = List(List("hi", "pandas"), List("hi holden"), List("bye"))
    val input2 = List(List("hi"), List("pandas"), List("byes"))
    val expected = List(List("pandas"), List("hi holden"), List("bye"))
    testOperation[String, String, String](input, input2, subtract _, expected, useSet = true)
  }

  def subtract(f1: DStream[String], f2: DStream[String]): DStream[String] = {
    f1.transformWith(f2, SampleStreamingTest.subtractRDDs _)
  }

  test("noop simple transformation") {
    def noop(s: DStream[String]) = s
    val input = List(List("hi"), List("hi holden"), List("bye"))
    testOperation[String, String](input, noop _, input, useSet = true)
  }

  test("a wrong expected multiset for a micro batch leads to a test fail") {
    val input = List(List("hi"), List("hi holden"), List("bye"))
    val badMultisetExpected = List(List("hi"), List("hi", "holden", "hi"), List("bye"))
    val thrown = intercept[TestFailedException] {
        testOperation[String, String](input, tokenize _, badMultisetExpected, useSet = true)
    }
  }
}

object SampleStreamingTest {
  def subtractRDDs(r1: RDD[String], r2: RDD[String]): RDD[String] = {
    r1.subtract(r2)
  }
}
