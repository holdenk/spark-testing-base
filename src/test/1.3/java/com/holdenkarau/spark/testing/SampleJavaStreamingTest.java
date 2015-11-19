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
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.SparkContext.*;

import org.scalatest.exceptions.TestFailedException;

import org.junit.Test;
import org.junit.Ignore;
import static org.junit.Assert.*;
import org.junit.runner.*;
import org.junit.runners.JUnit4;

public class SampleJavaStreamingTest extends JavaStreamingSuiteBase implements Serializable {
  static List<List<Integer>> input =  Arrays.asList(
    Arrays.asList(1,2), Arrays.asList(3,4,5));
  static List<List<Integer>> expectedOutput =  Arrays.asList(
    Arrays.asList(2), Arrays.asList(4));

  Function<JavaDStream<Integer>, JavaDStream<Integer>> filter =
    new Function<JavaDStream<Integer>, JavaDStream<Integer>>() {
      public JavaDStream<Integer> call(JavaDStream<Integer> myInput) {
        return myInput.filter(new Function<Integer, Boolean>() {
            public Boolean call(Integer x) { return x % 2 == 0;}});
      }
    };

  @Test public void verifyFilterTest() {
    testOperation(input, filter, expectedOutput);
  }

  @Test public void verifyFilterTestWithSet() {
    //testOperation(input, filter, SampleStreamingTest.expectedOutput, true);
  }
}
