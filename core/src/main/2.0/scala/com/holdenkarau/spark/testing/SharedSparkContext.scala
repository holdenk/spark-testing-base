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

import java.util.Date

import org.apache.spark._
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
 * Shares a local `SparkContext` between all tests in a suite
 * and closes it at the end. You can share between suites by enabling
 * reuseContextIfPossible.
 */
trait SharedSparkContext extends BeforeAndAfterAll with SparkContextProvider {
  self: Suite =>

  @transient private var _sc: SparkContext = _

  override def sc: SparkContext = _sc

  protected implicit def reuseContextIfPossible: Boolean = false

  override def beforeAll() {
    // This is kind of a hack, but if we've got an existing Spark Context
    // hanging around we need to kill it.
    if (!reuseContextIfPossible) {
      EvilSparkContext.stopActiveSparkContext()
    }
    _sc = SparkContext.getOrCreate(conf)
    setup(_sc)
    super.beforeAll()
  }

  override def afterAll() {
    try {
      if (!reuseContextIfPossible) {
        LocalSparkContext.stop(_sc)
        _sc = null
      }
    } finally {
      super.afterAll()
    }
  }
}
