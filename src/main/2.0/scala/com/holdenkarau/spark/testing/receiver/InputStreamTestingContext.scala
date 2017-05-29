package com.holdenkarau.spark.testing.receiver

import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
  * Sequences the execution of user-provided code that comprises one individual test of an
  * [[InputStreamSuiteBase]] test suite. Each individual test will typically instantiate one instance of this class.
  *
  * @param sparkContext        - a  [[com.holdenkarau.spark.testing.SharedSparkContext]] instance, provided by
  *                            [[com.holdenkarau.spark.testing.StreamingSuiteBase]], which will be used to create
  *                            an instance of  a [[org.apache.spark.streaming.StreamingContext]] that will be used
  *                            for a run of one individual test.
  *
  * @param dStreamCreationFunc - a user-provided code block that uses  the StreamingContext provided by this class
  *                            to generate a [[org.apache.spark.streaming.dstream.DStream]].
  *
  * @param testDataGenerationFunc - a user-provided code block that generates test data to be consumed by the
  *                               [[org.apache.spark.streaming.dstream.InputDStream]] under test.
  *
  * @param pauseDuration          - the amount of time to wait before kicking off [[testDataGenerationFunc]].
  *
  * @param expectedResult         - the number of results that we expect to find in the DStream produced
  *                               by [[InputStreamTestingContext.dStreamCreationFunc]]
  *
  * @param verboseOutput          - indicates whether internal INFO level logging info from Spark should appear or not
  *
  * @param streamingContextCreatorFunc - optional custom method to generate a [[StreamingContext]]
  *                                    from a [[SparkContext]]
  *
  * @param batchDuration        - duration of batch  cycle for streaming context
  *
  * @param awaitResultsTimeout - the amount of time to wait for user-provided Dstream generation code to yield results.
  *
  * @tparam T - the type of object written into the DStream under test.
  */
case class InputStreamTestingContext[T](sparkContext: SparkContext,
                                        dStreamCreationFunc: (StreamingContext) => DStream[T],
                                        testDataGenerationFunc: () => Unit,
                                        pauseDuration: scala.concurrent.duration.Duration,
                                        expectedResult: List[T],
                                        verboseOutput: Boolean = false,
                                        streamingContextCreatorFunc: Option[(SparkContext) => StreamingContext] = None,
                                        batchDuration : Duration = Seconds(1),
                                        awaitResultsTimeout : Duration =  Seconds(10)) {

  val ssc: StreamingContext = if (streamingContextCreatorFunc.isDefined) {
    streamingContextCreatorFunc.get(sparkContext)
  } else {
    new StreamingContext(sparkContext, batchDuration )
  }

  def run(): Unit = {
    ssc.sparkContext.setLogLevel(if (verboseOutput) "info" else "warn")
    val verifier =
      InputStreamVerifier[T](
        expectedResult.length,
        scala.concurrent.duration.Duration(s"${awaitResultsTimeout.milliseconds} ms"))
    verifier.runWithStreamingContext(ssc, dStreamCreationFunc)
    Thread.sleep(pauseDuration.toMillis) // give the code that creates the DStream time to start up
    testDataGenerationFunc() // generate test data that codeBlock will convert into a DStream[T]
    verifier.awaitAndVerifyResults(ssc, expectedResult)
  }
}
