package com.holdenkarau.spark.testing.receiver

import org.apache.spark.Logging
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{StreamingContext, StreamingContextState}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * Runs user-provided code that  generates a
  * [[org.apache.spark.streaming.dstream.DStream]], after which point
  * awaitAndVerifyResults() may be called to block until
  * [[com.holdenkarau.spark.testing.receiver.InputStreamVerifier.numExpectedResults]]
  * items appear in the directory where results are recorded.
  *
  * @param numExpectedResults  - the number of individual items that the
  *                            user-provided Dstream generation code is
  *                            expected to yield.
  * @param awaitResultsTimeout - the amount of time to wait for user-provided
  *                            Dstream generation code to yield results.
  * @tparam T - the type of object written into the DStream under test.
  */
case class InputStreamVerifier[T]
(numExpectedResults: Int,
 awaitResultsTimeout: Duration = Duration("10 second")
) extends Logging {

  val notifier: TestResultNotifier[T] =
    TestResultNotifierFactory.getForNResults(numExpectedResults)


  /**
    * Runs user-provided code that  generates
    * a [[org.apache.spark.streaming.dstream.DStream]].
    */

  def runWithStreamingContext(streamingContext: StreamingContext,
                              blockToRun: (StreamingContext) => DStream[T]): Unit = {

    try {
      val stream: DStream[T] = blockToRun(streamingContext)
      stream.foreachRDD {
        rdd => {
          rdd.foreach { case (item) =>
            notifier.recordResult(item)
          }
        }
      }
      Thread.sleep(200) // give some time to clean up (SPARK-1603)
      streamingContext.start()
    } catch {
      case e: Exception =>
        if (streamingContext.getState() == StreamingContextState.STOPPED) {
          streamingContext.stop(stopSparkContext = false)
        }
        throw e
    }
  }

  /**
    * Returns the Future[List] produced by calling
    * [[TestResultNotifier.getResults]], which in turn will
    * yield the results of executing the user-suplied
    * code block in [[runWithStreamingContext()]].
    */
  def awaitAndVerifyResults(streamingContext: StreamingContext,
                            expected: List[T]): Unit = {
    try {
      val result: Future[List[T]] = notifier.getResults
      val completedResult: Future[List[T]] = Await.ready(result, awaitResultsTimeout)
      val tuples = completedResult.value.get.get.toSet

      val expectedResultsAsSet = expected.toSet
      if (!tuples.equals(expectedResultsAsSet)) {
        throw new RuntimeException(
          s"actual result [ $tuples ] != expected result [ $expectedResultsAsSet ]")
      }
    } finally {
      streamingContext.stop(stopSparkContext = false)
      Thread.sleep(200) // give some time to clean up (SPARK-1603)
    }
  }
}
