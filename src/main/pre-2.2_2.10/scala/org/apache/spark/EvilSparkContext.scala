package org.apache.spark

import com.holdenkarau.spark.testing.LocalSparkContext

import java.util.concurrent.atomic.AtomicReference

/**
 * Access Spark internals.
 */
object EvilSparkContext {
  def stopActiveSparkContext(): Unit = {
    try {
      SparkContext.getOrCreate(null).stop()
    } catch {
      case _ => // pass
    }
  }
}
