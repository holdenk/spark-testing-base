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

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.random._
import org.scalacheck._

@Experimental
object RDDGenerator {

  /**
   * Generate an RDD of the desired type. Attempt to try different number of
   * partitions so as to catch problems with empty partitions, etc.
   *
   * minPartitions defaults to 1, but when generating data too large for a
   * single machine you must choose a larger value.
   *
   * @param sc            Spark Context
   * @param minPartitions defaults to 1
   * @param generator     used to create the generator.
   *                      This function will be used to create the generator as
   *                      many times as required.
   * @tparam T The required type for the RDD
   * @return
   */
  def genRDD[T: ClassTag](
    sc: SparkContext, minPartitions: Int = 1)(generator: => Gen[T]): Gen[RDD[T]] = {
    arbitraryRDD(sc, minPartitions)(generator).arbitrary
  }

  /**
    * Generate an RDD of the desired type with its size accessible in the lambda.
    * Attempt to try different number of partitions so as to catch problems with
    * empty partitions, etc.
    *
    * minPartitions defaults to 1, but when generating data too large for a
    * single machine you must choose a larger value.
    *
    * @param sc            Spark Context
    * @param minPartitions defaults to 1
    * @param generator     used to create the generator.
    *                      This function will be used to create the generator as
    *                      many times as required.
    * @tparam T The required type for the RDD
    * @return
    */
  def genSizedRDD[T: ClassTag](
    sc: SparkContext,
    minPartitions: Int = 1)(generator: Int => Gen[T]): Gen[RDD[T]] = {
    arbitrarySizedRDD(sc, minPartitions)(generator).arbitrary
  }

  /**
   * Generate an RDD of the desired type. Attempt to try different number of
   * partitions so as to catch problems with empty partitions, etc.
   *
   * minPartitions defaults to 1, but when generating data too large for a single
   * machine you must choose a larger value.
   *
   * @param sc            Spark Context
   * @param minPartitions defaults to 1
   * @param generator     used to create the generator.
   *                      This function will be used to create the generator as
   *                      many times as required.
   * @tparam T The required type for the RDD
   * @return
   */
  def arbitraryRDD[T: ClassTag](
    sc: SparkContext,
    minPartitions: Int = 1)(generator: => Gen[T]):
      Arbitrary[RDD[T]] = {
    Arbitrary {
      Gen.sized(sz =>
        arbitrary[T](sc, minPartitions, sz)(new WrappedGenerator[T](generator))
      )
    }
  }

  /**
    * Generate an RDD of the desired type with its size accessible in the lambda.
    * Attempt to try different number of
    * partitions so as to catch problems with empty partitions, etc.
    *
    * minPartitions defaults to 1, but when generating data too large for a single
    * machine you must choose a larger value.
    *
    * @param sc            Spark Context
    * @param minPartitions defaults to 1
    * @param generator     used to create the generator.
    *                      This function will be used to create the generator as
    *                      many times as required.
    * @tparam T The required type for the RDD
    * @return
    */
  def arbitrarySizedRDD[T: ClassTag](
    sc: SparkContext, minPartitions: Int = 1)
    (generator: Int => Gen[T]):
      Arbitrary[RDD[T]] = {
    Arbitrary {
      Gen.sized { sz =>
        arbitrary[T](
          sc,
          minPartitions,
          sz)(new WrappedSizedGenerator[T](sz, generator))
      }
    }
  }

  private def arbitrary[T: ClassTag](
    sc: SparkContext, minPartitions: Int, sz: Int)
    (randomDataGenerator: => RandomDataGenerator[T])
    : Gen[RDD[T]] = {
    sz match {
      case 0 => sc.emptyRDD[T]
      case size => {
        val mp = minPartitions
        val specialPartitionSizes = List(
          size, (size / 2), mp, mp + 1, mp + 3).filter(_ > mp)
        val partitionsGen =
          Gen.chooseNum(mp, 2 * size, specialPartitionSizes: _*)
        val rdds = partitionsGen.map { numPartitions =>
          RandomRDDs.randomRDD(sc, randomDataGenerator, size, numPartitions)
        }
        rdds
      }
    }
  }
}

/**
 * A WrappedGenerator wraps a ScalaCheck generator to allow Spark's
 * RandomRDD to use it.
 */
private[testing] class WrappedGenerator[T](getGenerator: => Gen[T])
    extends WrappedGen[T] {
  lazy val generator: Gen[T] = getGenerator

  def copy() = {
    new WrappedGenerator(generator)
  }
}

private[testing] class WrappedSizedGenerator[T]
  (size: Int, getGenerator: Int => Gen[T])
  extends WrappedGen[T] {
  lazy val generator: Gen[T] = getGenerator(size)

  def copy() = {
    new WrappedSizedGenerator(size, getGenerator)
  }
}

private[testing] trait WrappedGen[T] extends RandomDataGenerator[T] {
  val generator: Gen[T]
  lazy val params = Gen.Parameters.default
  lazy val random = new scala.util.Random()
  var seed: Option[rng.Seed] = None

  def nextValue(): T = {
    if (seed == None) {
      setSeed(random.nextLong())
    }
    generator(params, seed.get).get
  }

  override def setSeed(newSeed: Long): Unit = {
    seed = Some(rng.Seed.apply(newSeed))
    scala.util.Random.setSeed(newSeed)
  }
}
