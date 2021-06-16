package com.holdenkarau.spark.testing


import com.holdenkarau.spark.testing.receiver.InputStreamTestingContext

/**
  *
  * Provides support for testing [[org.apache.spark.streaming.dstream.InputDStream]]s
  * (which are typically associated with
  * [[org.apache.spark.streaming.receiver.Receiver]]s, hence the package name)
  * using the pattern below for each individual test in test suites extending from
  * [[com.holdenkarau.spark.testing.receiver.InputStreamSuiteBase]].
  *
  *
  *  - create a [[org.apache.spark.streaming.StreamingContext]] from the
  *  [[org.apache.spark.SparkContext]]
  *  provided by [com.holdenkarau.spark.testing.SharedSparkContext]]
  *
  *  - execute a user-provided code block that transforms the framework-provided
  *  StreamingContext into a
  *  [[org.apache.spark.streaming.dstream.DStream]]
  *  (see [[InputStreamTestingContext.dStreamCreationFunc()]])
  *
  *
  *  - after waiting for a configurable interval for the aforementioned code block
  *  to begin processing, execute a second block of code that generates test data to
  *  be consumed by the [[org.apache.spark.streaming.dstream.InputDStream]] under
  *  test. (see [[InputStreamTestingContext.pauseDuration]])
  *
  *  - compare the items encountered in the
  *  [[org.apache.spark.streaming.dstream.DStream]] produced by the first code
  *  block to a user specified list of expected items.
  *
  *  - stop the [[org.apache.spark.streaming.StreamingContext]] created during the
  *  first step.
  *
  *
  *  Implementation Overview
  *
  * The testing process is kicked off by the construction of a
  * InputStreamTestingContext, which wraps
  * user provided code and configuration options
  * (such as
  * [[InputStreamTestingContext.dStreamCreationFunc()]]), and the
  * [[InputStreamTestingContext.pauseDuration]])),
  *
  * The framework will grab each item produced by the
  * DStream and record it in the local file system using
  * a [[com.holdenkarau.spark.testing.receiver.TestResultNotifier]]
  *
  *
  * Known Issues:
  *
  *   -  verification against expected results ignores ordering of elements
  *   and disregards duplicates since the list of expected elements is
  *   turned into a set and compared against the actual results (which are
  *     also converted to a set.)
  *
  */
package object receiver { }
