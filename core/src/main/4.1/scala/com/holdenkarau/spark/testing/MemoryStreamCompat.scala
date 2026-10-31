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

package com.holdenkarau.spark.testing

import org.apache.spark.sql.{Encoder, SparkSession}
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream

/**
 * Spark 4.1 moved MemoryStream into
 * org.apache.spark.sql.execution.streaming.runtime and gave its factory a
 * SparkSession overload. The structured streaming helpers are shared across
 * every supported Spark version, so they come through here rather than naming
 * the package themselves.
 *
 * This is the 4.1+ variant.
 */
private[testing] object MemoryStreamCompat {
  type Stream[T] = MemoryStream[T]

  def create[T: Encoder](spark: SparkSession): Stream[T] =
    MemoryStream[T](implicitly[Encoder[T]], spark)
}
