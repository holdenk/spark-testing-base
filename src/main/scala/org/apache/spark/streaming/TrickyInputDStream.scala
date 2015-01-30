/*
 * This class exists because we do need to trick scala into giving us access to some private things
 */
package org.apache.spark.streaming.dstream
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import scala.reflect.ClassTag

class TrickyInputDStream[T: ClassTag](ssc_ : StreamingContext) extends InputDStream(ssc_) {
  def getZeroTime() = zeroTime
}
