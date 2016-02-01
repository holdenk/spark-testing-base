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

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue

import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import org.scalatest.FunSuite
import org.scalatest.exceptions.TestFailedException

/**
 * ArtisinalStreamingTest illustrates how to write a streaming test
 * without using spark-testing-bases' specialized streaming support.
 * This is not intended to be a class you should base your implementation
 * on, rather it is intended to disuade you from writing it by hand.
 * This does not use a manual clock and instead uses the kind of sketchy
 * sleep approach. Instead please look at [[SampleStreamingTest]].
 */
class ArtisinalStreamingTest extends FunSuite with SharedSparkContext {
  // tag::createQueueStream[]
  def makeSimpleQueueStream(ssc: StreamingContext) = {
    val input = List(List("hi"), List("happy pandas", "sad pandas"))
      .map(sc.parallelize(_))
    val idstream = ssc.queueStream(Queue(input:_*))
  }
  // end::createQueueStream[]

  // tag::HAPPY_PANDA[]
  test("artisinal streaming test") {
    val ssc = new StreamingContext(sc, Seconds(1))
    val input = List(List("hi"), List("happy pandas", "sad pandas"))
      .map(sc.parallelize(_))
    // Note: does not work for windowing or checkpointing
    val idstream = ssc.queueStream(Queue(input:_*))
    val tdstream = idstream.filter(_.contains("pandas"))
    val result = ArrayBuffer[String]()
    tdstream.foreach{(rdd: RDD[String], _) =>
      result ++= rdd.collect()
    }
    val startTime = System.currentTimeMillis()
    val maxWaitTime = 60 * 60 * 30
    ssc.start()
    while (result.size < 2 && System.currentTimeMillis() - startTime < maxWaitTime) {
      ssc.awaitTerminationOrTimeout(50)
    }
    ssc.stop(stopSparkContext = false)
    assert(List("happy pandas", "sad pandas") === result.toList)
  }
  // end::HAPPY_PANDA[]
}
