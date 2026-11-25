package org.apache.spark.streaming

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{InputDStream, QueueInputDStream}

import scala.collection.mutable
import scala.reflect.ClassTag


// TODO - Test with empty RDD
//

/**
  * Provides the [[constantStream]] method which  generates an
  * InputDStream whose configurable delay characteristics and
  * contents are useful for testing.
  */
class StreamingContextWithExtraInputStreamGenerators[T: ClassTag]
(sc_ : SparkContext,
 cp_ : Checkpoint,
 batchDuration_ : Duration
) extends TestStreamingContext(sc_, cp_, batchDuration_) {


  def constantStream(items: List[T],
                     generatorDelay: org.apache.spark.streaming.Duration)
  : InputDStream[T] = {
    val inputData = new ThrottledQueue[RDD[T]](generatorDelay)
    require(items.length >= 2)
    inputData.enqueue(sc.makeRDD(items))
    new QueueInputDStream[T](this, inputData, oneAtATime = true, defaultRDD = null)
  }
}

class ThrottledQueue[T](val dequeueDelay: Duration) extends mutable.Queue[T] {
  override def dequeue(): T = {
    Thread.sleep(dequeueDelay.milliseconds)
    super.dequeue()
  }
}
