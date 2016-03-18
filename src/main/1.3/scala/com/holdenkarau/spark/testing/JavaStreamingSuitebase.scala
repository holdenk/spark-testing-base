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

import org.apache.spark.api.java.function.{Function => JFunction, Function2 => JFunction2}
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
 * This implementation is designed to work with JUnit for java users.
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
  def verifyOutput[V: ClassTag](
      output: Seq[Seq[V]],
      expectedOutput: Seq[Seq[V]],
      ordered: Boolean): Unit = {

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
   * @param input          Sequence of input collections
   * @param operation      Binary DStream operation to be applied to the 2 inputs
   * @param expectedOutput Sequence of expected output collections
   */
  def testOperation[U, V](
      input: JList[JList[U]],
      operation: JFunction[JavaDStream[U], JavaDStream[V]],
      expectedOutput: JList[JList[V]]): Unit = {
    testOperation[U, V](input, operation, expectedOutput, false)
  }

  /**
   * Test unary DStream operation with a list of inputs, with number of
   * batches to run same as the number of input values.
   * You can simulate the input batch as a List of values or as null to simulate empty batch.
   *
   * @param input          Sequence of input collections
   * @param operation      Binary DStream operation to be applied to the 2 inputs
   * @param expectedOutput Sequence of expected output collections
   * @param ordered        Compare output values with expected output values
   *                       within the same output batch ordered or unordered.
   *                       Comparing doubles may not work well in case of unordered.
   */
  def testOperation[U, V](
      input: JList[JList[U]],
      operation: JFunction[JavaDStream[U], JavaDStream[V]],
      expectedOutput: JList[JList[V]],
      ordered: Boolean): Unit = {

    val numBatches = input.size

    implicit val ctagU = fakeClassTag[U]
    implicit val ctagV = fakeClassTag[V]

    val sInput = toSeq(input)
    val sExpectedOutput = toSeq(expectedOutput)

    def wrappedOperation(input: DStream[U]): DStream[V] = {
      operation.call(new JavaDStream[U](input)).dstream
    }

    withOutputAndStreamingContext(setupStreams[U, V](sInput, wrappedOperation)) {
      (outputStream, ssc) =>
        val output: Seq[Seq[V]] = runStreams[V](outputStream, ssc, numBatches, expectedOutput.size)
        verifyOutput[V](output, sExpectedOutput, ordered)
    }
  }


  /**
   * Test binary DStream operation with two lists of inputs, with number of
   * batches to run same as the number of input values. The size of the two input lists Should be the same.
   * You can simulate the input batch as a List of values or as null to simulate empty batch.
   *
   * @param input1         First sequence of input collections
   * @param input2         Second sequence of input collections
   * @param operation      Binary DStream operation to be applied to the 2 inputs
   * @param expectedOutput Sequence of expected output collections
   */
  def testOperation[U, V, W](
      input1: JList[JList[U]],
      input2: JList[JList[V]],
      operation: JFunction2[JavaDStream[U], JavaDStream[V], JavaDStream[W]],
      expectedOutput: JList[JList[W]]): Unit = {
    testOperation(input1, input2, operation, expectedOutput, false)
  }

  /**
   * Test binary DStream operation with two lists of inputs, with number of
   * batches to run same as the number of input values. The size of the two input lists Should be the same.
   * You can simulate the input batch as a List of values or as null to simulate empty batch.
   *
   * @param input1         First sequence of input collections
   * @param input2         Second sequence of input collections
   * @param operation      Binary DStream operation to be applied to the 2 inputs
   * @param expectedOutput Sequence of expected output collections
   * @param ordered        Compare output values with expected output values
   *                       within the same output batch ordered or unOrdered.
   *                       Comparing doubles may not work well in case of unordered.
   */
  def testOperation[U, V, W](
      input1: JList[JList[U]],
      input2: JList[JList[V]],
      operation: JFunction2[JavaDStream[U], JavaDStream[V], JavaDStream[W]],
      expectedOutput: JList[JList[W]],
      ordered: Boolean): Unit = {

    assertEquals("Length of the input lists are not equal", input1.length, input2.length)
    val numBatches = input1.size

    implicit val ctagU = fakeClassTag[U]
    implicit val ctagV = fakeClassTag[V]
    implicit val ctagW = fakeClassTag[W]

    val sInput1 = toSeq(input1)
    val sInput2 = toSeq(input2)
    val sExpectedOutput = toSeq(expectedOutput)

    def wrappedOperation(input1: DStream[U], input2: DStream[V]): DStream[W] = {
      operation.call(new JavaDStream[U](input1), new JavaDStream[V](input2)).dstream
    }

    withOutputAndStreamingContext(setupStreams[U, V, W](sInput1, sInput2, wrappedOperation)) {
      (outputStream, ssc) =>
        val output = runStreams[W](outputStream, ssc, numBatches, expectedOutput.size)
        verifyOutput[W](output, sExpectedOutput, ordered)
    }
  }

  private def toSeq[U](input: JList[JList[U]]) = input.map(_.toSeq).toSeq

  private def fakeClassTag[T]: ClassTag[T] = ClassTag.AnyRef.asInstanceOf[ClassTag[T]]

}
