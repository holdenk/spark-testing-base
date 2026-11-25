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

import org.scalatest.funsuite.AnyFunSuite

/**
 * Illustrate using per-test sample test. This is the one to use
 * when your tests may be destructive to the Spark context
 * (e.g. stopping it)
 */
class PerTestSampleTest extends AnyFunSuite with PerTestSparkContext {

  test("sample test stops a context") {
    sc.stop()
  }

  test("can still parallelize") {
    val input = List(1,2,3)
    assert(input === sc.parallelize(input).collect())
  }

}
