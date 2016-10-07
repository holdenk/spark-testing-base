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
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SQLContext;
import org.junit.*;

/** Shares a local `SparkContext` between all tests */
public class SharedJavaSparkContext implements SparkContextProvider {
  private static transient SparkSession _spark;
  private static transient SparkContext _sc;
  private static transient JavaSparkContext _jsc;
  private static transient SQLContext _sqlContext;
  protected boolean initialized = false;
  private static SparkConf _conf = new SparkConf().setMaster("local[4]").setAppName("magic");

  public SparkConf conf() {
    return _conf;
  }

  public SparkSession spark() {
    return _spark;
  }

  public SparkContext sc() {
    return _sc;
  }

  public JavaSparkContext jsc() {
    return _jsc;
  }

  public SQLContext sqlContext() {
    return _sqlContext;
  }

  /**
   * Hooks for setup code that needs to be executed/torn down in order with SparkContexts
   */
  void beforeAllTestCasesHook() {
  }

  static void afterAllTestCasesHook() {
  }

  @Before
  public void runBefore() {
    initialized = (_sc != null);

    if (!initialized) {
      _spark = SharedSparkContext$.MODULE$.createSparkSession();
      _sc = _spark.sparkContext();
      _jsc = new JavaSparkContext(_sc);
      _sqlContext = _spark.sqlContext();

      beforeAllTestCasesHook();
    }
  }

  @AfterClass
  static public void runAfterClass() {
    LocalSparkContext$.MODULE$.stop(_spark);
    _spark = null;
    _sc = null;
    _jsc = null;
    _sqlContext = null;
    afterAllTestCasesHook();
  }
}
