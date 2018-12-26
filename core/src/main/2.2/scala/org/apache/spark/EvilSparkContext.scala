package org.apache.spark

/**
 * Access Spark internals.
 */
object EvilSparkContext {
  def stopActiveSparkContext(): Unit = {
    SparkContext.getActive.foreach(_.stop())
  }
}
