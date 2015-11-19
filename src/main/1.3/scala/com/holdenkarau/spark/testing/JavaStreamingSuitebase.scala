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
import java.util.{List => JList}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.SynchronizedBuffer
import scala.collection.JavaConversions._
import scala.collection.immutable.{HashBag => Bag}
import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.api.java._
import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.streaming.scheduler.{StreamingListenerBatchStarted, StreamingListenerBatchCompleted, StreamingListener}
import org.apache.spark.streaming.util.TestManualClock
import org.apache.spark.{SparkConf, Logging}
import org.apache.spark.rdd.RDD

import org.junit._
import org.junit.Assert._

/**
  * This is the base trait for Spark Streaming testsuites. This provides basic functionality
  * to run user-defined set of input on user-defined stream operations, and verify the output.
 * This implementation is designer to work with JUnit for java users
 * Note: this always uses the manual clock
 */
class JavaStreamingSuiteBase extends JavaSuiteBase with StreamingSuiteCommon {

  override def conf = super.conf
    .set("spark.streaming.clock", "org.apache.spark.streaming.util.TestManualClock")

  override def runStreams[V: ClassTag](
    outputStream: TestOutputStream[V],
    ssc: TestStreamingContext,
    numBatches: Int,
    numExpectedOutput: Int
    ): Seq[Seq[V]] = {
    val output = super.runStreams(outputStream, ssc, numBatches, numExpectedOutput)
    assertEquals("Unexpected number of outputs generated", numExpectedOutput, output.size)
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
    assertEquals("Number of outputs do not match", expectedOutput.size, output.size)
    for (i <- 0 until output.size) {
      if (useSet) {
        implicit val config = Bag.configuration.compact[V]
        compareArrays[V](Bag(expectedOutput(i): _*).toArray,
          Bag(output(i): _*).toArray)
      } else {
        compareArrays[V](expectedOutput(i).toArray, output(i).toArray)
      }
    }
    logInfo("Output verified successfully")
  }

  /**
   * Test unary DStream operation with a list of inputs, with number of
   * batches to run same as the number of expected output values
   */
  def testOperation[U, V](
    input: JList[JList[U]],
    operation: JFunction[JavaDStream[U], JavaDStream[V]],
    expectedOutput: JList[JList[V]]) {
    testOperation[U, V](input, operation, expectedOutput, -1, false)
  }

  def testOperation[U, V](
    input: JList[JList[U]],
    operation: JFunction[JavaDStream[U], JavaDStream[V]],
    expectedOutput: JList[JList[V]],
    useSet: Boolean) {
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
  def testOperation[U, V](
    input: JList[JList[U]],
    operation: JFunction[JavaDStream[U], JavaDStream[V]],
    expectedOutput: JList[JList[V]],
    numBatches: Int,
    useSet: Boolean
  ) {
    val numBatches_ = if (numBatches > 0) numBatches else expectedOutput.size
    implicit val ctagU = ClassTag.AnyRef.asInstanceOf[ClassTag[U]]
    implicit val ctagV = ClassTag.AnyRef.asInstanceOf[ClassTag[V]]
    val sInput = input.map(_.toSeq).toSeq
    val sExpectedOutput = expectedOutput.map(_.toSeq).toSeq
    def wrapedOperation(input: DStream[U]): DStream[V] = {
      operation.call(new JavaDStream[U](input)).dstream
    }
    val output =
      withOutputAndStreamingContext(setupStreams[U, V](sInput, wrapedOperation)) {
        (outputStream, ssc) =>
        val output: Seq[Seq[V]] = runStreams[V](outputStream, ssc, numBatches_, expectedOutput.size)
        verifyOutput[V](output, sExpectedOutput, useSet)
      }
  }
}
