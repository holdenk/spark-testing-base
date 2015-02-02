package org.apache.spark.streaming.util

/*
 * Allows us access to a manual clock.
 */
class TestManualClock(clock: Clock) extends {
  val manualClock = clock.asInstanceOf[ManualClock]
  def currentTime() = {
    manualClock.currentTime()
  }
  val addToTime = manualClock.addToTime _
}
