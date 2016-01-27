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

import scala.reflect.{classTag, ClassTag}

import org.apache.spark.rdd._


object RDDComparisons {
  /**
   * Compare two RDDs. If they are equal returns None, otherwise
   * returns Some with the first mismatch. Requires that expected has an explicit partitioner.
   */
  def compareWithOrder[T: ClassTag](expected: RDD[T], result: RDD[T]): Option[(T, T)] = {
    if (result.partitioner.map(_ == expected.partitioner.get).getOrElse(false)) {
      compareWithOrderSamePartitioner(expected, result)
    } else {
      val targetPartitions = expected.partitions.size
      compareWithOrderSamePartitioner(expected.repartition(targetPartitions),
        result.repartition(targetPartitions))
    }
  }

  // tag::PANDA_ORDERED[]
  /**
   * Compare two RDDs. If they are equal returns None, otherwise
   * returns Some with the first mismatch. Assumes we have the same partitioner.
   */
  def compareWithOrderSamePartitioner[T: ClassTag](expected: RDD[T], result: RDD[T]): Option[(T, T)] = {
    expected.zip(result).filter{case (x, y) => x != y}.take(1).headOption
  }
  // end::PANDA_ORDERED[]

  // tag::PANDA_UNORDERED[]
  /**
   * Compare two RDDs where we do not require the order to be equal.
   * If they are equal returns None, otherwise returns Some with the first mismatch.
   */
  def compare[T: ClassTag](expected: RDD[T], result: RDD[T]): Option[(T, Int, Int)] = {
    // Key the values and count the number of each unique element
    val expectedKeyed = expected.map(x => (x, 1)).reduceByKey(_ + _)
    val resultKeyed = result.map(x => (x, 1)).reduceByKey(_ + _)
    // Group them together and filter for difference
    expectedKeyed.cogroup(resultKeyed).filter{case (_, (i1, i2)) =>
      i1.isEmpty || i2.isEmpty || i1.head != i2.head}.take(1).headOption.
      map{case (v, (i1, i2)) => (v, i1.headOption.getOrElse(0), i2.headOption.getOrElse(0))}
  }
  // end::PANDA_UNORDERED[]
}
