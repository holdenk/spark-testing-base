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

import com.holdenkarau.spark.testing.receiver.InputStreamTestingContext
import com.holdenkarau.spark.testing.receiver.InputStreamSuiteBase
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.scalatest.FunSuite


class InputStreamTest extends FunSuite with InputStreamSuiteBase {

  test("verify success if inputs received within expected timeout") {
    runTest(
      inputDataForDstream = List(1, 3),
      expectedResult = List(1, 3),
      generatorDelay = Seconds(0),
      awaitResultsTimeout = Seconds(10))
  }

  test("verify failure if inputs not received within expected timeout") {
    val thrown = intercept[Exception] {
      runTest(
        inputDataForDstream = List(1, 3),
        expectedResult = List(1, 3),
        generatorDelay = Seconds(10),
        awaitResultsTimeout = Seconds(0))
    }
    assert(thrown.getMessage contains "Futures timed out")
  }

  test("verify error raised if execpted results do not equal actual results") {
    val thrown = intercept[Exception] {
      runTest(inputDataForDstream = List(1, 5), expectedResult = List(1, 3))
    }
    assert(thrown.getMessage contains "!= expected result")
  }

  test("show that framework does not " +
    "currently detect duplicates and order discrepancies when checking results") {
    runTest(inputDataForDstream = List(2, 2, 1), expectedResult = List(1, 2))
  }

  def runTest(inputDataForDstream: List[Int],
              expectedResult: List[Int],
              batchDuration: Duration = Seconds(1),
              generatorDelay: Duration = Seconds(0),
              awaitResultsTimeout: Duration = Seconds(10)): Unit = {
    val dstreamCreationFunc: (StreamingContext) => DStream[Int] = {
      (ssc: StreamingContext) =>
        val customCtx =
          ssc.asInstanceOf[StreamingContextWithExtraInputStreamGenerators[Int]]
        customCtx.constantStream(inputDataForDstream, generatorDelay)
    }

    val noOpTestDataGenerationFunc: () => Unit = { () => }

    val streamingCtxCreator =
      (sc: SparkContext) =>
        new StreamingContextWithExtraInputStreamGenerators(sc, null, Duration(1000))

    InputStreamTestingContext(
      sparkContext = sc,
      dStreamCreationFunc = dstreamCreationFunc,
      testDataGenerationFunc = noOpTestDataGenerationFunc,
      pauseDuration = scala.concurrent.duration.Duration("1500 milliseconds"),
      expectedResult = expectedResult,
      streamingContextCreatorFunc = Some(streamingCtxCreator),
      awaitTimeout = awaitResultsTimeout,
      batchDuration = batchDuration)
      .run()
  }

}

