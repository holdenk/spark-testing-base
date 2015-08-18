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
 * Generator for RDDs for testing with ScalaCheck
 */
package com.holdenkarau.spark.testing

import scala.reflect.{ClassTag, classTag}

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.scalacheck._

@Experimental
object RDDGenerator {
  // Generate an RDD of the desired type. Attempt to try different number of partitions
  // so as to catch problems with empty partitions, etc.
  def genRDD[T: ClassTag](sc: SparkContext)(implicit a: Arbitrary[T]): Gen[RDD[T]] = {
    arbitraryRDD(sc).arbitrary
  }
  def arbitraryRDD[T: ClassTag](sc: SparkContext)(implicit a: Arbitrary[T]): Arbitrary[RDD[T]] = {
    Arbitrary {
      val genElem = for (e <- Arbitrary.arbitrary[T]) yield e
      def generateRDDOfSize(size: Int): Gen[RDD[T]] = {
        val special = List(size, (size/2).toInt, 2, 3)
        val myGen = for {
          n <- Gen.chooseNum(1, 2*size, special: _*)
          m <- Gen.listOfN(n, genElem)
        } yield (n, m)
        myGen.map{case (numPartitions, data) => sc.parallelize(data, numPartitions)}
      }
      def generateEmptyRDDOfType(): RDD[T] = {
        sc.emptyRDD[T]
      }
      Gen.sized(sz =>
        sz match {
          case 0 => generateEmptyRDDOfType()
          case _ => generateRDDOfSize(sz)
        })
    }
  }
}
