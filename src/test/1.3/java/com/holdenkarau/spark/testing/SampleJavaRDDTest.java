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

import java.io.*;
import java.util.List;
import java.util.Arrays;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.SparkContext.*;

import org.scalatest.exceptions.TestFailedException;

import org.junit.Test;
import org.junit.Ignore;
import static org.junit.Assert.*;
import org.junit.runner.*;
import org.junit.runners.JUnit4;

public class SampleJavaRDDTest extends SharedJavaSparkContext implements Serializable {
  @Test public void verifyMapTest() {
    List<Integer> input =  Arrays.asList(1,2);
    JavaRDD<Integer> inputRDD = jsc().parallelize(input);
    JavaRDD<Integer> result = inputRDD.map(
      new Function<Integer, Integer>() { public Integer call(Integer x) { return x*x;}});
    assertEquals(input.size(), result.count());
    }
}
