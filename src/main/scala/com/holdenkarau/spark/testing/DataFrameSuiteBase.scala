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

import scala.math.abs
import scala.util.hashing.MurmurHash3

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.StructType

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite

/**
 * :: Experimental ::
 * Base class for testing Spark DataFrames.
 */
trait DataFrameSuiteBase extends FunSuite with BeforeAndAfterAll
    with SharedSparkContext {
  val maxCount = 10
  @transient private var _sqlContext: SQLContext = _

  def sqlContext = _sqlContext

  override def beforeAll() {
    super.beforeAll()
    _sqlContext = org.apache.spark.sql.SQLContext.getOrCreate(sc)
  }

  override def afterAll() {
    super.afterAll()
    _sqlContext = null
  }

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
      !(r1.isEmpty || r2.isEmpty) && (!r1.head.equals(r2.head))
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
    val countSums = counts.scanLeft(0)(_ + _).zipWithIndex.map{case (x, y) => (y, x)}.toMap
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
      !(r1.isEmpty || r2.isEmpty) && (
        !DataFrameSuiteBase.approxEquals(r1.head, r2.head, tol))
    }.take(maxCount)
    assert(unequal === List())
  }

  /**
   * Compares the schema
   */
  def equalSchema(expected: StructType, result: StructType): Unit = {
    assert(expected.treeString === result.treeString)
  }
}

object DataFrameSuiteBase {
  /** Approximate equality, based on equals from [[Row]] */
  def approxEquals(r1: Row, r2: Row, tol: Double): Boolean = {
    if (r1.length != r2.length) {
      false
    } else {
      var i = 0
      val length = r1.length
      while (i < length) {
        if (r1.isNullAt(i) != r2.isNullAt(i)) {
          return false
        }
        if (!r1.isNullAt(i)) {
          val o1 = r1.get(i)
          val o2 = r2.get(i)
          o1 match {
            case b1: Array[Byte] =>
              if (!o2.isInstanceOf[Array[Byte]] ||
                !java.util.Arrays.equals(b1, o2.asInstanceOf[Array[Byte]])) {
                return false
              }
            case f1: Float if java.lang.Float.isNaN(f1) =>
              if (!o2.isInstanceOf[Float] || ! java.lang.Float.isNaN(o2.asInstanceOf[Float])) {
                return false
              }
            case d1: Double if java.lang.Double.isNaN(d1) =>
              if (!o2.isInstanceOf[Double] || ! java.lang.Double.isNaN(o2.asInstanceOf[Double])) {
                return false
              }
            case d1: java.math.BigDecimal if o2.isInstanceOf[java.math.BigDecimal] =>
              if (d1.compareTo(o2.asInstanceOf[java.math.BigDecimal]) != 0) {
                return false
              }
            case f1: Float => if (abs(f1-o2.asInstanceOf[Float]) > tol) {
              return false
            }
            case d1: Double => if (abs(d1-o2.asInstanceOf[Double]) > tol) {
              return false
            }
            case _ => if (o1 != o2) {
              return false
            }
          }
        }
        i += 1
      }
    }
    true
  }
}
