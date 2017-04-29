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

/*
 * This is a subset of the Spark Utils object that we use for testing
 */
package com.holdenkarau.spark.testing

import org.apache.spark.rdd._
import org.scalatest.Suite

import scala.reflect.ClassTag


trait RDDComparisons extends RDDComparisonsLike with TestSuite { self: Suite =>
}

trait RDDComparisonsLike extends TestSuiteLike {
  // tag::PANDA_ORDERED[]
  /**
   * Asserts two RDDs are equal (with the same order).
   * If they are equal assertion succeeds, otherwise assertion fails.
   */
  def assertRDDEqualsWithOrder[T: ClassTag](
    expected: RDD[T], result: RDD[T]): Unit = {
    assertTrue(compareRDDWithOrder(expected, result).isEmpty)
  }

  /**
   * Compare two RDDs with order (e.g. [1,2,3] != [3,2,1])
   * If the partitioners are not the same this requires multiple passes
   * on the input.
   * If they are equal returns None, otherwise returns Some with the first mismatch.
   * If the lengths are not equal, one of the two components may be None.
   */
  def compareRDDWithOrder[T: ClassTag](
    expected: RDD[T], result: RDD[T]): Option[(Option[T], Option[T])] = {
    // If there is a known partitioner just zip
    if (result.partitioner.map(_ == expected.partitioner.get).getOrElse(false)) {
      compareRDDWithOrderSamePartitioner(expected, result)
    } else {
      // Otherwise index every element
      def indexRDD[T](rdd: RDD[T]): RDD[(Long, T)] = {
        rdd.zipWithIndex.map { case (x, y) => (y, x) }
      }
      val indexedExpected = indexRDD(expected)
      val indexedResult = indexRDD(result)
      indexedExpected.cogroup(indexedResult).filter { case (_, (i1, i2)) =>
        i1.isEmpty || i2.isEmpty || i1.head != i2.head
      }.take(1).headOption.
        map { case (_, (i1, i2)) =>
          (i1.headOption, i2.headOption) }.take(1).headOption
    }
  }

  /**
   * Compare two RDDs. If they are equal returns None, otherwise
   * returns Some with the first mismatch. Assumes we have the same partitioner.
   */
  def compareRDDWithOrderSamePartitioner[T: ClassTag](
    expected: RDD[T], result: RDD[T]): Option[(Option[T], Option[T])] = {
    // Handle mismatched lengths by converting into options and padding with Nones
    expected.zipPartitions(result) {
      (thisIter, otherIter) =>
        new Iterator[(Option[T], Option[T])] {
          def hasNext: Boolean = (thisIter.hasNext || otherIter.hasNext)

          def next(): (Option[T], Option[T]) = {
            (thisIter.hasNext, otherIter.hasNext) match {
              case (false, true) => (Option.empty[T], Some(otherIter.next()))
              case (true, false) => (Some(thisIter.next()), Option.empty[T])
              case (true, true) => (Some(thisIter.next()), Some(otherIter.next()))
              case _ => throw new Exception("next called when elements consumed")
            }
          }
        }
    }.filter { case (v1, v2) => v1 != v2 }.take(1).headOption
  }

  // end::PANDA_ORDERED[]

  // tag::PANDA_UNORDERED[]
  /**
   * Asserts two RDDs are equal (un ordered).
   * If they are equal assertion succeeds, otherwise assertion fails.
   */
  def assertRDDEquals[T: ClassTag](expected: RDD[T], result: RDD[T]): Unit = {
    assertTrue(compareRDD(expected, result).isEmpty)
  }

  /**
   * Compare two RDDs where we do not require the order to be equal.
   * If they are equal returns None, otherwise returns Some with the first mismatch.
   *
   * @return None if the two RDDs are equal, or Some containing
   *              the first mismatch information.
   *              The mismatch information will be Tuple3 of:
   *              (key, number of times this key occur in expected RDD,
   *              number of times this key occur in result RDD)
   */
  def compareRDD[T: ClassTag](expected: RDD[T], result: RDD[T]):
      Option[(T, Int, Int)] = {
    // Key the values and count the number of each unique element
    val expectedKeyed = expected.map(x => (x, 1)).reduceByKey(_ + _)
    val resultKeyed = result.map(x => (x, 1)).reduceByKey(_ + _)
    // Group them together and filter for difference
    expectedKeyed.cogroup(resultKeyed).filter { case (_, (i1, i2)) =>
      i1.isEmpty || i2.isEmpty || i1.head != i2.head
    }
      .take(1).headOption.
      map { case (v, (i1, i2)) =>
        (v, i1.headOption.getOrElse(0), i2.headOption.getOrElse(0)) }
  }

  // end::PANDA_UNORDERED[]
}
