package org.apache.spark

/**
 * Access Spark internals.
 */
object EvilSparkContext {
  def stopActiveSparkContext(): Unit = {
    // This is very slow and creates a SparkContext if one doesn't exist
    // it is less than ideal but the active context is hidden from us in 2.X
    SparkContext.getOrCreate().stop()
  }
}
