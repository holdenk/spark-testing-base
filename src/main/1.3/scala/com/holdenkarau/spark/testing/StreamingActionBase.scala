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

import org.apache.spark.streaming.TestStreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.scheduler.{
  StreamingListenerBatchCompleted, StreamingListener}
import org.apache.spark.streaming.util.TestManualClock
import org.scalatest.Suite

import scala.reflect.ClassTag

/**
 * Methods for testing Spark actions. Because actions don't return a DStream, you
 * will need to verify the results of your test against mocks.
 */
trait StreamingActionBase extends StreamingSuiteBase {
  self: Suite =>


  val batchCountListener = new BatchCountListener

  /**
   * Execute unary DStream operation with a list of inputs and no expected output
   *
   * @param input     Sequence of input collections
   * @param operation Unary DStream operation to be applied to the input
   */
  def runAction[U: ClassTag](input: Seq[Seq[U]], operation: DStream[U] => Unit) {

    val numBatches_ = input.size
    withStreamingContext(setupStream[U](input, operation)) { ssc =>
      runActionStream(ssc, numBatches_)
    }
  }

  private def withStreamingContext(outputStreamSSC: TestStreamingContext)
      (block: TestStreamingContext => Unit): Unit = {
    try {
      block(outputStreamSSC)
    } finally {
      try {
        outputStreamSSC.stop(stopSparkContext = false)
      } catch {
        case e: Throwable =>
          logError("Error stopping StreamingContext", e)
      }
    }
  }

  private def setupStream[U: ClassTag](input: Seq[Seq[U]],
      operation: DStream[U] => Any): TestStreamingContext = {

    // Create TestStreamingContext
    val ssc = new TestStreamingContext(sc, batchDuration)
    ssc.addStreamingListener(batchCountListener)
    if (checkpointDir != null) {
      ssc.checkpoint(checkpointDir)
    }

    // Setup the stream computation
    val inputStream = createTestInputStream(sc, ssc, input)
    operation(inputStream)
    ssc
  }

  private def runActionStream(ssc: TestStreamingContext, numBatches: Int) {
    assert(numBatches > 0, "Number of batches to run stream computation is zero")
    batchCountListener.batchCount = 0

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

    // wait for expected number of batches to execute
    val startTime = System.currentTimeMillis()
    while (batchCountListener.batchCount < numBatches &&
      System.currentTimeMillis() - startTime < maxWaitTimeMillis) {
      logInfo(s"batches: run = ${batchCountListener.batchCount} " +
        s"target = ${numBatches}")
      ssc.awaitTerminationOrTimeout(50)
    }
    val timeTaken = System.currentTimeMillis() - startTime
    logInfo("Output generated in " + timeTaken + " milliseconds")

    Thread.sleep(100) // Give some time for the forgetting old RDDs to complete
  }

}

class BatchCountListener extends StreamingListener {
  var batchCount = 0

  override def onBatchCompleted(
    batchCompleted: StreamingListenerBatchCompleted): Unit = {
    batchCount = batchCount + 1
  }
}
