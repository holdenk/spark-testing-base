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
import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import org.apache.spark.streaming.dstream.FriendlyInputDStream

/**
 * This is a input stream just for the testsuites. This is equivalent to a
 * checkpointable, replayable, reliable message queue like Kafka.
 * It requires a sequence as input, and returns the i_th element at the i_th batch
 * under manual clock.
 *
 * Based on TestInputStream class from TestSuiteBase in the Apache Spark project.
 */
class TestInputStream[T: ClassTag](@transient var sc: SparkContext,
  ssc_ : StreamingContext, input: Seq[Seq[T]], numPartitions: Int)
  extends FriendlyInputDStream[T](ssc_) {

  def start(): Unit = {}

  def stop(): Unit = {}

  def compute(validTime: Time): Option[RDD[T]] = {
    logInfo("Computing RDD for time " + validTime)
    val index = ((validTime - ourZeroTime) / slideDuration - 1).toInt
    val selectedInput = if (index < input.size) input(index) else Seq[T]()

    // lets us test cases where RDDs are not created
    Option(selectedInput).map{si =>
      val rdd = sc.makeRDD(si, numPartitions)
      logInfo("Created RDD " + rdd.id + " with " + selectedInput)
      rdd
    }
  }
}
