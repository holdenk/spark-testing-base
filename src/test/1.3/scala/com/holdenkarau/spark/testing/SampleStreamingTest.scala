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

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream._
import org.scalactic.Equality
import org.scalatest.exceptions.TestFailedException

class SampleStreamingTest extends StreamingSuiteBase {

  //tag::simpleStreamingTest[]
  test("really simple transformation") {
    val input = List(List("hi"), List("hi holden"), List("bye"))
    val expected = List(List("hi"), List("hi", "holden"), List("bye"))
    testOperation[String, String](input, tokenize _, expected, ordered = false)
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
    testOperation[String, String, String](input, input2, subtract _, expected, ordered = false)
  }

  def subtract(f1: DStream[String], f2: DStream[String]): DStream[String] = {
    f1.transformWith(f2, SampleStreamingTest.subtractRDDs _)
  }

  test("noop simple transformation") {
    def noop(s: DStream[String]) = s
    val input = List(List("hi"), List("hi holden"), List("bye"))
    testOperation[String, String](input, noop _, input, ordered = false)
  }

  test("a wrong expected multiset for a micro batch leads to a test fail") {
    val input = List(List("hi"), List("hi holden"), List("bye"))
    val badMultisetExpected = List(List("hi"), List("hi", "holden", "hi"), List("bye"))
    intercept[TestFailedException] {
        testOperation[String, String](input, tokenize _, badMultisetExpected, ordered = false)
    }
  }

  test("custom equality object (String)") {
    val input = List(List("hi"), List("hi holden"), List("bye"))
    val expected = List(List("Hi"), List("hI", "HoLdeN"), List("bYe"))

    implicit val stringCustomEquality =
      new Equality[String] {
        override def areEqual(a: String, b: Any): Boolean =
          b match {
            case s: String => a.equalsIgnoreCase(s)
            case _ => false
          }
      }

    testOperation[String, String](input, tokenize _, expected, ordered = true)
    testOperation[String, String](input, tokenize _, expected, ordered = false)
  }

  test("custom equality object (Integer)") {
    val input = List(List(-1), List(-2, 3, -4), List(5, -6))
    val expected = List(List(1), List(2, 3, 4), List(5, 6))

    implicit val integerCustomEquality =
      new Equality[Int] {
        override def areEqual(a: Int, b: Any): Boolean =
          b match {
            case n: Int => Math.abs(a) == Math.abs(n)
            case _ => false
          }
      }

    def doNothing(ds: DStream[Int]) = ds

    testOperation[Int, Int](input, doNothing _, expected, ordered = false)
    testOperation[Int, Int](input, doNothing _, expected, ordered = true)
  }

  test("CountByWindow with windowDuration 3s and slideDuration=2s") {
    // There should be 2 windows :  {batch2, batch1},  {batch4, batch3, batch2}
    val batch1 = List("a", "b")
    val batch2 = List("d", "f", "a")
    val batch3 = List("f", "g"," h")
    val batch4 = List("a")
    val input= List(batch1, batch2, batch3, batch4)
    val expected = List(List(5L), List(7L))

    def countByWindow(ds:DStream[String]):DStream[Long] = {
      ds.countByWindow(windowDuration = Seconds(3), slideDuration = Seconds(2))
    }

    testOperation[String, Long](input, countByWindow _, expected, ordered = true)
  }

  test("two lists length should be equal") {
    def nothing(stream1: DStream[Int], stream2: DStream[Int]) = stream1

    val input1 = List(List(1), List(2))
    val input2 = List(List(1), List(2), List(3))

    val output = List(List(1), List(2), List(3))

    intercept[TestFailedException] {
      testOperation(input1, input2, nothing _, output, ordered = false)
    }
  }
}

object SampleStreamingTest {
  def subtractRDDs(r1: RDD[String], r2: RDD[String]): RDD[String] = {
    r1.subtract(r2)
  }
}
