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

import java.io._

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.util.TestManualClock
import org.apache.spark.{Logging, SparkConf, _}
import org.scalatest.concurrent.Eventually.timeout
import org.scalatest.concurrent.PatienceConfiguration
import org.scalatest.time.{Seconds => ScalaTestSeconds, Span}

import scala.collection.mutable.{ArrayBuffer, SynchronizedBuffer}
import scala.language.implicitConversions
import scala.reflect.ClassTag

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
 * Shared logic between the Java & Scala Streaming suites.
 */
private[holdenkarau] trait StreamingSuiteCommon extends Logging with SparkContextProvider {
  // tag::createTestInputStream[]
  /**
   * Create an input stream for the provided input sequence. This is done using
   * TestInputStream as queueStream's are not checkpointable.
   */
  private[holdenkarau] def createTestInputStream[T: ClassTag](sc: SparkContext, ssc_ : TestStreamingContext,
    input: Seq[Seq[T]]): TestInputStream[T] = {
    new TestInputStream(sc, ssc_, input, numInputPartitions)
  }
  // end::createTestInputStream[]

  // Batch duration
  def batchDuration: Duration = Seconds(1)

  // Name of the framework for Spark context
  def framework: String = this.getClass.getSimpleName

  // Number of partitions of the input parallel collections created for testing
  def numInputPartitions: Int = 2

  // Maximum time to wait before the test times out
  def maxWaitTimeMillis: Int = 10000

  // Whether to use manual clock or not
  def useManualClock: Boolean = true

  // Whether to actually wait in real time before changing manual clock
  def actuallyWait: Boolean = false

  def master = "local[4]"

  // Directory where the checkpoint data will be saved
  lazy val checkpointDir = {
    val dir = Utils.createTempDir()
    logDebug(s"checkpointDir: $dir")
    dir.toString
  }


  // A SparkConf to use in tests. Can be modified before calling setupStreams to configure things.
  override def conf = new SparkConf()
    .setMaster(master)
    .setAppName(framework)
    .set("spark.streaming.clock", "org.apache.spark.streaming.util.TestManualClock")

  // Timeout for use in ScalaTest `eventually` blocks
  val eventuallyTimeout: PatienceConfiguration.Timeout = timeout(Span(10, ScalaTestSeconds))

  /**
   * Run a block of code with the given StreamingContext and automatically
   * stop the context when the block completes or when an exception is thrown.
   */
  private[holdenkarau] def withOutputAndStreamingContext[R](outputStreamSSC: (TestOutputStream[R], TestStreamingContext))
    (block: (TestOutputStream[R], TestStreamingContext) => Unit): Unit = {
    val outputStream = outputStreamSSC._1
    val ssc = outputStreamSSC._2
    try {
      ssc.start()
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
  private[holdenkarau] def setupStreams[U: ClassTag, V: ClassTag](
    input: Seq[Seq[U]],
    operation: DStream[U] => DStream[V]
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
  private[holdenkarau] def setupStreams[U: ClassTag, V: ClassTag, W: ClassTag](
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
  private[holdenkarau] def runStreams[V: ClassTag](
    outputStream: TestOutputStream[V],
    ssc: TestStreamingContext,
    numBatches: Int,
    numExpectedOutput: Int
    ): Seq[Seq[V]] = {
    assert(numBatches > 0, "Number of batches to run stream computation is zero")
    assert(numExpectedOutput > 0, "Number of expected outputs after " + numBatches + " is zero")
    logInfo("numBatches = " + numBatches + ", numExpectedOutput = " + numExpectedOutput)

    val output = outputStream.output

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
      ssc.awaitTerminationOrTimeout(50)
    }
    val timeTaken = System.currentTimeMillis() - startTime
    logInfo("Output generated in " + timeTaken + " milliseconds")
    output.foreach(x => logInfo("[" + x.mkString(",") + "]"))
    assert(timeTaken < maxWaitTimeMillis, "Operation timed out after " + timeTaken + " ms")
    Thread.sleep(100) // Give some time for the forgetting old RDDs to complete

    output.toSeq
  }

  private[holdenkarau] def setupClock() = {
    if (useManualClock) {
      logInfo("Using manual clock")
      conf.set("spark.streaming.clock", "org.apache.spark.streaming.util.TestManualClock")
    } else {
      logInfo("Using real clock")
      conf.set("spark.streaming.clock", "org.apache.spark.streaming.util.SystemClock")
    }
  }
}
