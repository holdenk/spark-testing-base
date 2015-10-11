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
import scala.collection.mutable.Queue
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
class TestOutputStream[T: ClassTag](parent: DStream[T],
  val output: ArrayBuffer[Seq[T]] = ArrayBuffer[Seq[T]]()) extends Serializable {
  parent.foreachRDD{(rdd: RDD[T], time) =>
    val collected = rdd.collect()
    output += collected
  }
}


/**
  * This is the base trait for Spark Streaming testsuites. This provides basic functionality
  * to run user-defined set of input on user-defined stream operations, and verify the output.
  */
trait StreamingSuiteBase extends FunSuite with BeforeAndAfterAll with Logging
    with SharedSparkContext {

  // Name of the framework for Spark context
  def framework: String = this.getClass.getSimpleName

  // Master for Spark context
  def master: String = "local[4]"

  // Batch duration
  def batchDuration: Duration = Seconds(1)

  // Directory where the checkpoint data will be saved
  lazy val checkpointDir = {
    val dir = Utils.createTempDir()
    logDebug(s"checkpointDir: $dir")
    dir.toString
  }

  // tag::createTestInputStream
  /**
   * Create an input stream for the provided input sequence. This is done using
   * TestInputStream as queueStream's are not checkpointable.
   */
  def createTestInputStream[T: ClassTag](sc: SparkContext, ssc_ : TestStreamingContext,
    input: Seq[Seq[T]]): TestInputStream[T] = {
    new TestInputStream(sc, ssc_, input, numInputPartitions)
  }
  // end::createTestInputStream

  // Number of partitions of the input parallel collections created for testing
  def numInputPartitions: Int = 2

  // Maximum time to wait before the test times out
  def maxWaitTimeMillis: Int = 10000

  // Whether to use manual clock or not
  def useManualClock: Boolean = true

  // Whether to actually wait in real time before changing manual clock
  def actuallyWait: Boolean = false

  // A SparkConf to use in tests. Can be modified before calling setupStreams to configure things.
  override val conf = new SparkConf()
    .setMaster(master)
    .setAppName(framework)

  // Timeout for use in ScalaTest `eventually` blocks
  val eventuallyTimeout: PatienceConfiguration.Timeout = timeout(Span(10, ScalaTestSeconds))

  // Default before function for any streaming test suite. Override this
  // if you want to add your stuff to "before" (i.e., don't call before { } )
  override def beforeAll() {
    if (useManualClock) {
      logInfo("Using manual clock")
      conf.set("spark.streaming.clock", "org.apache.spark.streaming.util.TestManualClock")
    } else {
      logInfo("Using real clock")
      conf.set("spark.streaming.clock", "org.apache.spark.streaming.util.SystemClock")
    }
    super.beforeAll()
  }

  // Default after function for any streaming test suite. Override this
  // if you want to add your stuff to "after" (i.e., don't call after { } )
  override def afterAll() {
    System.clearProperty("spark.streaming.clock")
    super.afterAll()
  }


  /**
   * Run a block of code with the given StreamingContext and automatically
   * stop the context when the block completes or when an exception is thrown.
   */
  def withOutputAndStreamingContext[R](outputStreamSSC: (TestOutputStream[R], TestStreamingContext))
    (block: (TestOutputStream[R], TestStreamingContext) => Unit): Unit = {
    val outputStream = outputStreamSSC._1
    val ssc = outputStreamSSC._2
    try {
      block(outputStream, ssc)
    } finally {
      try {
        ssc.stop(stopSparkContext = false)
      } catch {
        case e: Exception =>
          logError("Error stopping StreamingContext", e)
      }
    }
  }


  /**
   * Set up required DStreams to test the DStream operation using the two sequences
   * of input collections.
   */
  def setupStreams[U: ClassTag, V: ClassTag](
    input: Seq[Seq[U]],
    operation: DStream[U] => DStream[V],
    numPartitions: Int = numInputPartitions
  ): (TestOutputStream[V], TestStreamingContext) = {
    // Create TestStreamingContext
    val ssc = new TestStreamingContext(sc, batchDuration)
    if (checkpointDir != null) {
      ssc.checkpoint(checkpointDir)
    }

    // Setup the stream computation
    val inputStream = createTestInputStream(sc, ssc, input)
    val operatedStream = operation(inputStream)
    val outputStream = new TestOutputStream[V](operatedStream,
      new ArrayBuffer[Seq[V]] with SynchronizedBuffer[Seq[V]])
    (outputStream, ssc)
  }

  /**
   * Set up required DStreams to test the binary operation using the sequence
   * of input collections.
   */
  def setupStreams[U: ClassTag, V: ClassTag, W: ClassTag](
    input1: Seq[Seq[U]],
    input2: Seq[Seq[V]],
    operation: (DStream[U], DStream[V]) => DStream[W]
  ): (TestOutputStream[W], TestStreamingContext) = {
    // Create StreamingContext
    val ssc = new TestStreamingContext(sc, batchDuration)
    if (checkpointDir != null) {
      ssc.checkpoint(checkpointDir)
    }

    // Setup the stream computation
    val inputStream1 = createTestInputStream(sc, ssc, input1)
    val inputStream2 = createTestInputStream(sc, ssc, input2)
    val operatedStream = operation(inputStream1, inputStream2)
    val outputStream = new TestOutputStream[W](operatedStream,
      new ArrayBuffer[Seq[W]] with SynchronizedBuffer[Seq[W]])
    (outputStream, ssc)
  }

  /**
   * Runs the streams set up in `ssc` on manual clock for `numBatches` batches and
   * returns the collected output. It will wait until `numExpectedOutput` number of
   * output data has been collected or timeout (set by `maxWaitTimeMillis`) is reached.
   *
   * Returns a sequence of items for each RDD.
   */
  def runStreams[V: ClassTag](
    outputStream: TestOutputStream[V],
    ssc: TestStreamingContext,
    numBatches: Int,
    numExpectedOutput: Int
    ): Seq[Seq[V]] = {
    assert(numBatches > 0, "Number of batches to run stream computation is zero")
    assert(numExpectedOutput > 0, "Number of expected outputs after " + numBatches + " is zero")
    logInfo("numBatches = " + numBatches + ", numExpectedOutput = " + numExpectedOutput)

    val output = outputStream.output

    try {
      // Start computation
      ssc.start()

      // Advance manual clock
      val clock = ssc.getScheduler().clock.asInstanceOf[TestManualClock]
      logInfo("Manual clock before advancing = " + clock.currentTime())
      if (actuallyWait) {
        for (i <- 1 to numBatches) {
          logInfo("Actually waiting for " + batchDuration)
          clock.addToTime(batchDuration.milliseconds)
          Thread.sleep(batchDuration.milliseconds)
        }
      } else {
        clock.addToTime(numBatches * batchDuration.milliseconds)
      }
      logInfo("Manual clock after advancing = " + clock.currentTime())

      // Wait until expected number of output items have been generated
      val startTime = System.currentTimeMillis()
      while (output.size < numExpectedOutput &&
        System.currentTimeMillis() - startTime < maxWaitTimeMillis) {
        logInfo("output.size = " + output.size + ", numExpectedOutput = " + numExpectedOutput)
        ssc.awaitTermination(50)
      }
      val timeTaken = System.currentTimeMillis() - startTime
      logInfo("Output generated in " + timeTaken + " milliseconds")
      output.foreach(x => logInfo("[" + x.mkString(",") + "]"))
      assert(timeTaken < maxWaitTimeMillis, "Operation timed out after " + timeTaken + " ms")
      assert(output.size === numExpectedOutput, "Unexpected number of outputs generated")

      Thread.sleep(100) // Give some time for the forgetting old RDDs to complete
    } finally {
      ssc.stop(stopSparkContext = false)
    }
    output.toSeq
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
