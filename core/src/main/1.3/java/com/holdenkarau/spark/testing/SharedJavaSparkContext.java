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
package com.holdenkarau.spark.testing;

import org.apache.spark.*;
import org.apache.spark.api.java.*;
import org.junit.*;

/** Shares a local `SparkContext` between all tests in a suite and closes it at the end */
public class SharedJavaSparkContext implements SparkContextProvider {
  private static transient SparkContext _sc;
  private static transient JavaSparkContext _jsc;
  protected boolean initialized = false;

  // Since we want to support java 7 for now we can't use the default method from the trait directly.
  @Override public void setup(SparkContext sc) {
    SparkContextProvider$class.setup(this, sc);
  }

  @Override public String appID() {
    return SparkContextProvider$class.appID(this);
  }

  @Override public SparkConf conf() {
    return SparkContextProvider$class.conf(this);
  }

  public SparkContext sc() {
    return _sc;
  }

  public JavaSparkContext jsc() {
    return _jsc;
  }

  /**
   * Hooks for setup code that needs to be executed/torn down in order with SparkContexts
   */
  protected void beforeAllTestCasesHook() {
  }

  protected static void afterAllTestCasesHook() {
  }

  @Before
  public void runBefore() {
    initialized = (_sc != null);

    if (!initialized) {
      _sc = new SparkContext(conf());
      _jsc = new JavaSparkContext(_sc);

      beforeAllTestCasesHook();
    }
  }

  @AfterClass
  static public void runAfterClass() {
    LocalSparkContext$.MODULE$.stop(_sc);
    _sc = null;
    _jsc = null;
    afterAllTestCasesHook();
  }
}
