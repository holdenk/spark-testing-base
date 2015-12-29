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

import scala.util.Random

class SampleRDDTest extends FunSuite with SharedSparkContext {
  test("really simple transformation") {
    val input = List("hi", "hi holden", "bye")
    val expected = List(List("hi"), List("hi", "holden"), List("bye"))

    assert(tokenize(sc.parallelize(input)).collect().toList === expected)
  }
  def tokenize(f: RDD[String]) = {
    f.map(_.split(" ").toList)
  }

  test("really simple transformation with rdd - rdd comparision") {
    val inputList = List("hi", "hi holden", "bye")
    val inputRDD = tokenize(sc.parallelize(inputList))

    val expectedList = List(List("hi"), List("hi", "holden"), List("bye"))
    val expectedRDD = sc.parallelize(expectedList)

    assert(None === RDDComparisions.compare(expectedRDD, inputRDD))
  }

  test("empty RDD compare") {
    val inputList = List[String]()
    val inputRDD = sc.parallelize(inputList)

    assert(None === RDDComparisions.compare(inputRDD, inputRDD))
  }

  test("simple equal compare") {
    val inputList = List("ab", "bc", "bc", "cd")
    val inputRDD = sc.parallelize(inputList)

    val expected = Random.shuffle(inputList)
    val expectedRDD = sc.parallelize(expected)
    println(expected)

    assert(None === RDDComparisions.compare(expectedRDD, inputRDD))
  }

  test("complex equal compare") {
    val inputList = List(("ab", 4), "bc", "bc", ("cd", 6), ("ab", 4), "hanafy", 55)
    val inputRDD = sc.parallelize(inputList)

    val expected = Random.shuffle(inputList)
    val expectedRDD = sc.parallelize(expected)

    assert(None === RDDComparisions.compare(expectedRDD, inputRDD))
  }

  test("not equal compare") {
    val inputList = List("ab", 1)
    val inputRDD = sc.parallelize(inputList)

    val expectedList = List("ab", -1)
    val expectedRDD = sc.parallelize(expectedList)

    assert(None !== RDDComparisions.compare(expectedRDD, inputRDD))
  }

  test("empty RDD compareWithOrder") {
    val inputList = List[String]()
    val inputRDD = sc.parallelize(inputList)

    assert(None === RDDComparisions.compareWithOrder(inputRDD, inputRDD))
  }

  test("equal compareWithOrder") {
    val inputList = List("ab", "bc", "holden", (1, "wxyz"), 22, "abo trika")
    val inputRDD = sc.parallelize(inputList)

    val expectedRDD = sc.parallelize(inputList)

    assert(None === RDDComparisions.compareWithOrder(inputRDD, expectedRDD))
  }

  test("not equal compareWithOrder") {
    val inputList = List(1, 2, 3, 4)
    val inputRDD = sc.parallelize(inputList)

    val expectedList = List(2, 1, 3, 4)
    val expectedRDD = sc.parallelize(expectedList)

    assert(None !== RDDComparisions.compareWithOrder(inputRDD, expectedRDD))
  }
}
