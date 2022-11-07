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

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.junit.Test;
import scala.Option;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SampleJavaRDDTest extends SharedJavaSparkContext implements Serializable {
  @Test
  public void verifyMapTest() {
    List<Integer> input = Arrays.asList(1, 2);
    JavaRDD<Integer> inputRDD = jsc().parallelize(input);
    JavaRDD<Integer> result = inputRDD.map(
      new Function<Integer, Integer>() { public Integer call(Integer x) { return x*x;}});
    assertEquals(input.size(), result.count());
  }

  @Test
  public void compareWithOrderExpectedSuccess() {
    JavaRDD<Integer> rdd = jsc().parallelize(Arrays.asList(1, 2));
    Option<Tuple2<Option<Integer>, Option<Integer>>> result = JavaRDDComparisons.compareRDDWithOrder(rdd, rdd);
    assertTrue(result.isEmpty());
    JavaRDDComparisons.assertRDDEqualsWithOrder(rdd, rdd);
  }

  @Test
  public void compareWithoutOrderExpectedSuccess() {
    JavaRDD<String> rdd1 = jsc().parallelize(Arrays.asList("Hello", "It's", "Me"));
    JavaRDD<String> rdd2 = jsc().parallelize(Arrays.asList("It's", "Me", "Hello"));

    Option<Tuple3<String, Integer, Integer>> result = JavaRDDComparisons.compareRDD(rdd1, rdd2);
    assertTrue(result.isEmpty());
    JavaRDDComparisons.assertRDDEquals(rdd1, rdd2);
  }

  @Test
  public void assertEqualsExpectedSuccess() {
    JavaRDD<Integer> rdd = jsc().parallelize(Arrays.asList(1, 2, 3, 4));
    JavaRDDComparisons.assertRDDEquals(rdd, rdd);
    JavaRDDComparisons.assertRDDEqualsWithOrder(rdd, rdd);
  }

  @Test
  public void compareWithOrderExpectedFailure() {
    JavaRDD<Integer> rdd1 = jsc().parallelize(Arrays.asList(1, 2, 3, 4));
    JavaRDD<Integer> rdd2 = jsc().parallelize(Arrays.asList(1, 2, 3, 5));

    Option<Tuple2<Option<Integer>, Option<Integer>>> result = JavaRDDComparisons.compareRDDWithOrder(rdd1, rdd2);
    assertTrue(result.isDefined());
  }

  @Test
  public void compareWithoutOrderExpectedFailure() {
    JavaRDD<String> rdd1 = jsc().parallelize(Arrays.asList("Hello", "It's", "Me"));
    JavaRDD<String> rdd2 = jsc().parallelize(Arrays.asList("Hello", "It's", "YOU"));

    Option<Tuple3<String, Integer, Integer>> result = JavaRDDComparisons.compareRDD(rdd1, rdd2);
    assertTrue(result.isDefined());
  }

  @Test(expected = java.lang.AssertionError.class)
  public void assertEqualsWithOrderExpectedFailure() {
    JavaRDD<Integer> rdd1 = jsc().parallelize(Arrays.asList(1, 2, 3, 4));
    JavaRDD<Integer> rdd2 = jsc().parallelize(Arrays.asList(4, 3, 2, 1));

    JavaRDDComparisons.assertRDDEqualsWithOrder(rdd1, rdd2);
  }

  @Test(expected = java.lang.AssertionError.class)
  public void assertEqualsWithoutOrderExpectedFailure() {
    JavaRDD<Integer> rdd1 = jsc().parallelize(Arrays.asList(1, 2, 3, 4));
    JavaRDD<Integer> rdd2 = jsc().parallelize(Arrays.asList(5, 3, 2, 1));

    JavaRDDComparisons.assertRDDEquals(rdd1, rdd2);
  }

}
