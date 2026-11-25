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

package org.apache.spark.streaming
import org.apache.spark.streaming.scheduler._
import org.apache.spark._

/**
 * Extend the StreamingContext so we can be a little evil and poke at its
 * internals.
 */
class TestStreamingContext (
    sc_ : SparkContext,
    cp_ : Checkpoint,
    batchDur_ : Duration
  ) extends StreamingContext(sc_, cp_, batchDur_) {
  /**
    * Create a TestableStreamingContext using an existing SparkContext.
    * @param sparkContext existing SparkContext
    * @param batchDuration the time interval at which streaming data will be divided
    *        into batches
    */
  def this(sparkContext: SparkContext, batchDuration: Duration) = {
    this(sparkContext, null, batchDuration)
  }
  def getScheduler(): JobScheduler = scheduler
}
