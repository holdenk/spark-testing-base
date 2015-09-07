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

import scala.util.hashing.MurmurHash3

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.StructType

import org.scalatest.FunSuite

/**
 * :: Experimental ::
 * Base class for testing Spark DataFrames.
 */
trait DataFrameSuiteBase extends FunSuite {
  val maxCount = 10

  /**
   * Compares if two [[DataFrame]]s are equal, checks the schema and then if that matches
   * checks if the rows are equal. May compute underlying DataFrame multiple times.
   */
  def equalDataFrames(expected: DataFrame, result: DataFrame) {
    equalSchema(expected.schema, result.schema)
    val expectedRDD = zipWithIndex(expected.rdd)
    val resultRDD = zipWithIndex(result.rdd)
    assert(expectedRDD.count() == resultRDD.count())
    val unequal = expectedRDD.cogroup(resultRDD).filter{case (idx, (r1, r2)) =>
      !(r1.isEmpty || r2.isEmpty) && (r1.head.equals(r2.head))
    }.take(maxCount)
    assert(unequal === List())
  }


  /**
   * Zip RDD's with precise indexes. This is used so we can join two DataFrame's
   * Rows together regardless of if the source is different but still compare based on
   * the order.
   */
  private def zipWithIndex[T](input: RDD[T]): RDD[(Int, T)] = {
    val counts = input.mapPartitions{itr => Iterator(itr.size)}.collect()
    val countSums = counts.scanLeft(0)(_ + _).zipWithIndex.map{case (x, y) => (y,x)}.toMap
    input.mapPartitionsWithIndex{case (idx, itr) => itr.zipWithIndex.map{case (y, i) =>
      (i + countSums(idx), y)}
    }
  }

  /**
   * Compares if two [[DataFrame]]s are equal, checks that the schemas are the same.
   * When comparing inexact fields uses tol.
   */
  def approxEqualDataFrames(expected: DataFrame, result: DataFrame, tol: Double) {
    equalSchema(expected.schema, result.schema)
    val expectedRDD = zipWithIndex(expected.rdd)
    val resultRDD = zipWithIndex(result.rdd)
    val unequal = expectedRDD.cogroup(resultRDD).filter{case (idx, (r1, r2)) =>
      !(r1.isEmpty || r2.isEmpty) && (r1.head.equals(r2.head))
    }.take(maxCount)
    assert(unequal === List())
  }

  /**
   * Compares the schema
   */
  def equalSchema(expected: StructType, result: StructType) = {
    assert(expected.treeString === result.treeString)
  }
}
