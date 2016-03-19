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

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class SampleJavaStreamingTest extends JavaStreamingSuiteBase implements Serializable {

  @Test
  public void verifyFilterTest() {
    List<List<Integer>> input = Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4, 5));
    List<List<Integer>> expectedOutput = Arrays.asList(Arrays.asList(2), Arrays.asList(4));

    testOperation(input, filterOddOperation, expectedOutput);
    testOperation(input, filterOddOperation, expectedOutput, true);
  }

  @Test(expected = java.lang.AssertionError.class)
  public void wrongUnaryOperation() {
    List<List<Integer>> input = Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4, 5));
    List<List<Integer>> expectedOutput = Arrays.asList(Arrays.asList(-2), Arrays.asList(4));

    testOperation(input, filterOddOperation, expectedOutput);
  }

  @Test
  public void verifyBinaryOperation() {
    List<List<Integer>> input1 = Arrays.asList(Arrays.asList(1), Arrays.asList(3));
    List<List<Integer>> input2 = Arrays.asList(Arrays.asList(2), Arrays.asList(4));
    List<List<Integer>> expectedOutput = Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4));

    testOperation(input1, input2, unionOperation, expectedOutput);
    testOperation(input1, input2, unionOperation, expectedOutput, true);
  }

  @Test(expected = java.lang.AssertionError.class)
  public void wrongBinaryOperation() {
    List<List<Integer>> input1 = Arrays.asList(Arrays.asList(1), Arrays.asList(3));
    List<List<Integer>> input2 = Arrays.asList(Arrays.asList(2), Arrays.asList(4));
    List<List<Integer>> expectedOutput = Arrays.asList(Arrays.asList(1, 2, 3), Arrays.asList(3, 4));

    testOperation(input1, input2, unionOperation, expectedOutput);
    testOperation(input1, input2, unionOperation, expectedOutput, true);
  }

  private final Function<JavaDStream<Integer>, JavaDStream<Integer>> filterOddOperation =
    new Function<JavaDStream<Integer>, JavaDStream<Integer>>() {
      public JavaDStream<Integer> call(JavaDStream<Integer> myInput) {
        return myInput.filter(new Function<Integer, Boolean>() {
          public Boolean call(Integer x) {
            return x % 2 == 0;
          }
        });
      }
    };

  private final Function2<JavaDStream<Integer>, JavaDStream<Integer>, JavaDStream<Integer>> unionOperation =
    new Function2<JavaDStream<Integer>, JavaDStream<Integer>, JavaDStream<Integer>>() {
      public JavaDStream<Integer> call(JavaDStream<Integer> input1, JavaDStream<Integer> input2) {
        return input1.union(input2);
      }
    };

}
