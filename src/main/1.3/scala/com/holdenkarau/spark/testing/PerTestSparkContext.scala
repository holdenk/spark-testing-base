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

import org.apache.spark._

import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Suite

/**
 * Provides a local `sc`
 * {@link SparkContext} variable, correctly stopping it after each test.
 * The stopping logic is provided in {@link LocalSparkContext}.
 */
trait PerTestSparkContext extends LocalSparkContext with BeforeAndAfterEach
    with SparkContextProvider { self: Suite =>

  override def beforeEach() {
    sc = new SparkContext(conf)
    setup(sc)
    super.beforeEach()
  }

  override def afterEach() {
    super.afterEach()
  }
}
