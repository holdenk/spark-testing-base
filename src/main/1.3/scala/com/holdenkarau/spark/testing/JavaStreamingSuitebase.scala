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

import java.util.{List => JList}

import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.streaming.api.java._
import org.apache.spark.streaming.dstream.DStream
import org.junit.Assert._

import scala.collection.JavaConversions._
import scala.collection.immutable.{HashBag => Bag}
import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
  * This is the base trait for Spark Streaming testsuite. This provides basic functionality
  * to run user-defined set of input on user-defined stream operations, and verify the output.
  * This implementation is designer to work with JUnit for java users.
  *
  * Note: this always uses the manual clock
  */
class JavaStreamingSuiteBase extends JavaSuiteBase with StreamingSuiteCommon {

  override def conf = super.conf
    .set("spark.streaming.clock", "org.apache.spark.streaming.util.TestManualClock")

  /**
   * Verify whether the output values after running a DStream operation
   * is same as the expected output values, by comparing the output
   * collections either as lists (order matters) or sets (order does not matter)
   */
  def verifyOutput[V: ClassTag](output: Seq[Seq[V]],
                                expectedOutput: Seq[Seq[V]],
                                ordered: Boolean) {
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
    for (i <- output.indices) {
      if (ordered) {
        compareArrays[V](expectedOutput(i).toArray, output(i).toArray)
      } else {
        implicit val config = Bag.configuration.compact[V]
        compareArrays[V](Bag(expectedOutput(i): _*).toArray, Bag(output(i): _*).toArray)
      }
    }

    logInfo("Output verified successfully")
  }

  /**
    * Test unary DStream operation with a list of inputs, with number of
    * batches to run same as the number of input values.
    * You can simulate the input batch as a List of values or as null to simulate empty batch.
    *
    * @param input Sequence of input collections
    * @param operation Binary DStream operation to be applied to the 2 inputs
    * @param expectedOutput Sequence of expected output collections
    */
  def testOperation[U, V](input: JList[JList[U]],
                          operation: JFunction[JavaDStream[U], JavaDStream[V]],
                          expectedOutput: JList[JList[V]]) {
    testOperation[U, V](input, operation, expectedOutput, false)
  }

  /**
    * Test unary DStream operation with a list of inputs, with number of
    * batches to run same as the number of input values.
    * You can simulate the input batch as a List of values or as null to simulate empty batch.
    *
    * @param input Sequence of input collections
    * @param operation Binary DStream operation to be applied to the 2 inputs
    * @param expectedOutput Sequence of expected output collections
    * @param ordered Compare the output values with the expected output values ordered or not.
    *                Comparing doubles may not work well in case of unordered.
    */
  def testOperation[U, V](input: JList[JList[U]],
                          operation: JFunction[JavaDStream[U], JavaDStream[V]],
                          expectedOutput: JList[JList[V]],
                          ordered: Boolean) {

    val numBatches = input.size

    implicit val ctagU = ClassTag.AnyRef.asInstanceOf[ClassTag[U]]
    implicit val ctagV = ClassTag.AnyRef.asInstanceOf[ClassTag[V]]

    val sInput = input.map(_.toSeq).toSeq
    val sExpectedOutput = expectedOutput.map(_.toSeq).toSeq

    def wrappedOperation(input: DStream[U]): DStream[V] = {
      operation.call(new JavaDStream[U](input)).dstream
    }

    withOutputAndStreamingContext(setupStreams[U, V](sInput, wrappedOperation)) {
      (outputStream, ssc) =>
      val output: Seq[Seq[V]] = runStreams[V](outputStream, ssc, numBatches, expectedOutput.size)
      verifyOutput[V](output, sExpectedOutput, ordered)
    }
  }
}
