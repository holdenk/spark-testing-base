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

import scala.collection.mutable
import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.spark.Logging
import org.apache.spark.streaming.dstream.DStream
import org.scalactic.Equality
import org.scalatest.{BeforeAndAfterAll, FunSuite}

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

  /**
   * Verify whether the output values after running a DStream operation
   * is same as the expected output values, by comparing the output
   * collections either as lists (order matters) or sets (order does not matter)
   *
   * @param ordered Compare output values with expected output values
   *                within the same output batch ordered or unordered.
   *                Comparing doubles may not work well in case of unordered.
   */
  def verifyOutput[V: ClassTag](
      output: Seq[Seq[V]],
      expectedOutput: Seq[Seq[V]],
      ordered: Boolean
    ) (implicit equality: Equality[V]): Unit = {

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
    if (ordered) {
      for (i <- output.indices)
        equalsOrdered(output(i), expectedOutput(i))

    } else {
      for (i <- output.indices)
        equalsUnordered(output(i), expectedOutput(i))
    }

    logInfo("Output verified successfully")
  }

  private def equalsUnordered[V](output: Seq[V], expected: Seq[V])(implicit equality: Equality[V]) = {
    assert(output.length === expected.length)

    val length = output.length
    val set = new mutable.BitSet(length)

    for (i <- 0 until length) {
      val equalElements = (0 until length).filter(x => (!set.contains(x) && output(i) === expected(x))).take(1)

      if (equalElements.isEmpty)
        assert(output === expected) // only to show the two unequal lists to user

      set += equalElements(0)
    }
  }

  private def equalsOrdered[V](output: Seq[V], expected: Seq[V])(implicit equality: Equality[V]) = {
    assert(output.length === expected.length)
    for (i <- output.indices)
      assert(output(i) === expected(i))
  }

  // Wrappers with ordered = false
  def testOperation[U: ClassTag, V: ClassTag](
      input: Seq[Seq[U]],
      operation: DStream[U] => DStream[V],
      expectedOutput: Seq[Seq[V]]
  ) (implicit equality: Equality[V]): Unit = {
    testOperation(input, operation, expectedOutput, false)
  }

  def testOperation[U: ClassTag, V: ClassTag, W: ClassTag](
      input1: Seq[Seq[U]],
      input2: Seq[Seq[V]],
      operation: (DStream[U], DStream[V]) => DStream[W],
      expectedOutput: Seq[Seq[W]]
  ) (implicit equality: Equality[W]): Unit = {
    testOperation(input1, input2, operation, expectedOutput, false)
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
  def testOperation[U: ClassTag, V: ClassTag](
      input: Seq[Seq[U]],
      operation: DStream[U] => DStream[V],
      expectedOutput: Seq[Seq[V]],
      ordered: Boolean
    ) (implicit equality: Equality[V]): Unit = {
    val numBatches = input.size

    withOutputAndStreamingContext(setupStreams[U, V](input, operation)) { (outputStream, ssc) =>
      val output: Seq[Seq[V]] = runStreams[V](outputStream, ssc, numBatches, expectedOutput.size)
      verifyOutput[V](output, expectedOutput, ordered)
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
   * @param ordered        Compare output values with expected output values
   *                       within the same output batch ordered or unOrdered.
   *                       Comparing doubles may not work well in case of unordered.
   */
  def testOperation[U: ClassTag, V: ClassTag, W: ClassTag](
      input1: Seq[Seq[U]],
      input2: Seq[Seq[V]],
      operation: (DStream[U], DStream[V]) => DStream[W],
      expectedOutput: Seq[Seq[W]],
      ordered: Boolean
    ) (implicit equality: Equality[W]): Unit = {
    assert(input1.length === input2.length, "Length of the input lists are not equal")

    val numBatches = input1.size

    withOutputAndStreamingContext(setupStreams[U, V, W](input1, input2, operation)) {
      (outputStream, ssc) =>
        val output = runStreams[W](outputStream, ssc, numBatches, expectedOutput.size)
        verifyOutput[W](output, expectedOutput, ordered)
    }
  }
}
