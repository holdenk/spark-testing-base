package org.apache.spark.streaming
import org.apache.spark.streaming.scheduler._
import org.apache.spark._

/* Extend the StreamingContext so we can be a little evil and poke at its internals */

class TestStreamingContext (
    sc_ : SparkContext,
    cp_ : Checkpoint,
    batchDur_ : Duration
  ) extends StreamingContext(sc_, cp_, batchDur_) {
  /**
    * Create a TestableStreamingContext using an existing SparkContext.
    * @param sparkContext existing SparkContext
    * @param batchDuration the time interval at which streaming data will be divided into batches
    */
  def this(sparkContext: SparkContext, batchDuration: Duration) = {
    this(sparkContext, null, batchDuration)
  }
  def getScheduler() = scheduler
}
