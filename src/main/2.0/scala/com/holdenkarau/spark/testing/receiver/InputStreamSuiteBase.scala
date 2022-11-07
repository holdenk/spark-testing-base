package com.holdenkarau.spark.testing.receiver

import com.holdenkarau.spark.testing.StreamingSuiteBase
import org.apache.spark.SparkConf
import org.scalatest.FunSuite


/**
  * Supports unit testing of [[org.apache.spark.streaming.dstream.InputDStream]]s
  * wherein each test will typically
  *
  *   - perform some set up of test fixtures
  *
  *   - create an instance of an [[InputStreamTestingContext]] and invoke the
  *   run() method of that instance.
  *
  * The parent trait --  [[StreamingSuiteBase]] -- serves to provide the
  * [[org.apache.spark.SparkContext]] from which
  * a [[org.apache.spark.streaming.StreamingContext]] will be created and stopped
  * for each individual test. The [[org.apache.spark.SparkContext]]
  * is re-used for each test in the suite.
  */
trait InputStreamSuiteBase extends FunSuite with StreamingSuiteBase {
  /**
    *  Use system clock rather than TestManualClock, otherwise receivers never run.
    */
  override def conf: SparkConf = {
    val confToTweak = super.conf
    confToTweak.set(
      "spark.streaming.clock",
      "org.apache.spark.streaming.util.SystemClock")
    confToTweak
  }
}
