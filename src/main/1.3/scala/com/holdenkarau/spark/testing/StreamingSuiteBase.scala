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
import org.apache.spark.SparkContext._

import java.io._

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.SynchronizedBuffer
import scala.collection.immutable.{HashBag => Bag}
import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.time.{Span, Seconds => ScalaTestSeconds}
import org.scalatest.concurrent.Eventually.timeout
import org.scalatest.concurrent.PatienceConfiguration

import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.scheduler.{StreamingListenerBatchStarted, StreamingListenerBatchCompleted, StreamingListener}
import org.apache.spark.streaming.util.TestManualClock
import org.apache.spark.{SparkConf, Logging}
import org.apache.spark.rdd.RDD


/**
  * This is a output stream just for testing.
  *
  * The buffer contains a sequence of RDD's, each containing a sequence of items
  */
// tag::collectResults[]
class TestOutputStream[T: ClassTag](parent: DStream[T],
  val output: ArrayBuffer[Seq[T]] = ArrayBuffer[Seq[T]]()) extends Serializable {
  parent.foreachRDD{(rdd: RDD[T], time) =>
    val collected = rdd.collect()
    output += collected
  }
}
// end::collectResults[]

/**
  * This is the base trait for Spark Streaming testsuites. This provides basic functionality
  * to run user-defined set of input on user-defined stream operations, and verify the output.
  */
trait StreamingSuiteBase extends FunSuite with BeforeAndAfterAll with Logging
    with StreamingSuiteCommon with SharedSparkContext {

  // Default before function for any streaming test suite. Override this
  // if you want to add your stuff to "before" (i.e., don't call before { } )
  override def beforeAll() {
    setupClock()
    super.beforeAll()
  }

  // Default after function for any streaming test suite. Override this
  // if you want to add your stuff to "after" (i.e., don't call after { } )
  override def afterAll() {
    System.clearProperty("spark.streaming.clock")
    super.afterAll()
  }

  override def runStreams[V: ClassTag](
    outputStream: TestOutputStream[V],
    ssc: TestStreamingContext,
    numBatches: Int,
    numExpectedOutput: Int
    ): Seq[Seq[V]] = {
    val output = super.runStreams(outputStream, ssc, numBatches, numExpectedOutput)
    assert(output.size === numExpectedOutput, "Unexpected number of outputs generated")
    output
  }

  /**
   * Verify whether the output values after running a DStream operation
   * is same as the expected output values, by comparing the output
   * collections either as lists (order matters) or sets (order does not matter)
   */
  def verifyOutput[V: ClassTag](
    output: Seq[Seq[V]],
    expectedOutput: Seq[Seq[V]],
    useSet: Boolean
  ) {
    logInfo("--------------------------------")
    logInfo("output.size = " + output.size)
    logInfo("output")
    output.foreach(x => logInfo("[" + x.mkString(",") + "]"))
    logInfo("expected output.size = " + expectedOutput.size)
    logInfo("expected output")
    expectedOutput.foreach(x => logInfo("[" + x.mkString(",") + "]"))
    logInfo("--------------------------------")

    // Match the output with the expected output
    assert(output.size === expectedOutput.size, "Number of outputs do not match")
    for (i <- 0 until output.size) {
      if (useSet) {
        implicit val config = Bag.configuration.compact[V]
        assert(Bag(output(i): _*) === Bag(expectedOutput(i): _*))
      } else {
        assert(output(i).toList === expectedOutput(i).toList)
      }
    }
    logInfo("Output verified successfully")
  }

  /**
   * Test unary DStream operation with a list of inputs, with number of
   * batches to run same as the number of expected output values
   */
  def testOperation[U: ClassTag, V: ClassTag](
    input: Seq[Seq[U]],
    operation: DStream[U] => DStream[V],
    expectedOutput: Seq[Seq[V]],
    useSet: Boolean = false
  ) {
    testOperation[U, V](input, operation, expectedOutput, -1, useSet)
  }

  /**
   * Test unary DStream operation with a list of inputs
   * @param input      Sequence of input collections
   * @param operation  Binary DStream operation to be applied to the 2 inputs
   * @param expectedOutput Sequence of expected output collections
   * @param numBatches Number of batches to run the operation for
   * @param useSet     Compare the output values with the expected output values
   *                   as sets (order matters) or as lists (order does not matter)
   */
  def testOperation[U: ClassTag, V: ClassTag](
    input: Seq[Seq[U]],
    operation: DStream[U] => DStream[V],
    expectedOutput: Seq[Seq[V]],
    numBatches: Int,
    useSet: Boolean
  ) {
    val numBatches_ = if (numBatches > 0) numBatches else expectedOutput.size
    val output =
      withOutputAndStreamingContext(setupStreams[U, V](input, operation)) { (outputStream, ssc) =>
        val output: Seq[Seq[V]] = runStreams[V](outputStream, ssc, numBatches_, expectedOutput.size)
        verifyOutput[V](output, expectedOutput, useSet)
      }
  }

  /**
   * Test binary DStream operation with two lists of inputs, with number of
   * batches to run same as the number of expected output values
   */
  def testOperation[U: ClassTag, V: ClassTag, W: ClassTag](
    input1: Seq[Seq[U]],
    input2: Seq[Seq[V]],
    operation: (DStream[U], DStream[V]) => DStream[W],
    expectedOutput: Seq[Seq[W]],
    useSet: Boolean
  ) {
    testOperation[U, V, W](input1, input2, operation, expectedOutput, -1, useSet)
  }

  /**
   * Test binary DStream operation with two lists of inputs
   * @param input1     First sequence of input collections
   * @param input2     Second sequence of input collections
   * @param operation  Binary DStream operation to be applied to the 2 inputs
   * @param expectedOutput Sequence of expected output collections
   * @param numBatches Number of batches to run the operation for
   * @param useSet     Compare the output values with the expected output values
   *                   as sets (order matters) or as lists (order does not matter)
   */
  def testOperation[U: ClassTag, V: ClassTag, W: ClassTag](
    input1: Seq[Seq[U]],
    input2: Seq[Seq[V]],
    operation: (DStream[U], DStream[V]) => DStream[W],
    expectedOutput: Seq[Seq[W]],
    numBatches: Int,
    useSet: Boolean
  ) {
    val numBatches_ = if (numBatches > 0) numBatches else expectedOutput.size
    withOutputAndStreamingContext(setupStreams[U, V, W](input1, input2, operation)) {
      (outputStream, ssc) =>
      val output = runStreams[W](outputStream, ssc, numBatches_, expectedOutput.size)
      verifyOutput[W](output, expectedOutput, useSet)
    }
  }
}
