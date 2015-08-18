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
import org.apache.spark.api.java._
import org.junit._

/** Shares a local `SparkContext` between all tests in a suite and closes it at the end */
class SharedJavaSparkContext {
  @transient private SparkContext _sc = null;
  @transient private JavaSparkContext _jsc = null;

  SparkContext sc() {
    return _sc;
  }

  JavaSparkContext jsc() {
    return _jsc;
  }

  SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("test")

  @BeforeClass
  static void runBeforeClass() {
    _sc = new SparkContext(conf)
    _jsc = new JavaSparkContext(_sc)
  }

  @AfterClass
  static void runAfterClass() {
    LocalSparkContext.stop(_sc)
    _sc = null
    _jsc = null
  }
}
