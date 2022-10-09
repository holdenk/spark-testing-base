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

import breeze.linalg.NumericOps.Arrays
import org.junit.Assert._
import spire.ClassTag

import java.util

class JavaSuiteBase extends SharedJavaSparkContext {
  /**
   * Utility wrapper around assertArrayEquals that resolves the types
   */
  def compareArrays[U: ClassTag](i1: Array[U], i2: Array[U], sorted: Boolean = false): Unit = {
    val (arr1, arr2) = if(sorted) (copyAndSort(i1), copyAndSort(i2)) else (i1, i2)
    (arr1, arr2) match {
      case (a1: Array[Long], a2: Array[Long]) => assertArrayEquals(a1, a2)
      case (a1: Array[Int], a2: Array[Int]) => assertArrayEquals(a1, a2)
      case (a1: Array[Short], a2: Array[Short]) => assertArrayEquals(a1, a2)
      case (a1: Array[Char], a2: Array[Char]) => assertArrayEquals(a1, a2)
      case (a1: Array[Byte], a2: Array[Byte]) => assertArrayEquals(a1, a2)
      case (a1: Array[Object], a2: Array[Object]) => assertArrayEquals(a1, a2)
    }
  }

  def copyAndSort[U: ClassTag](array: Array[U]): Array[U] = {
    val copyArray = Array.ofDim[U](array.length)
    Array.copy(array, 0, copyArray, 0, array.length)
    copyArray match {
      case a: Array[Long] => util.Arrays.sort(a)
      case a: Array[Int] => util.Arrays.sort(a)
      case a: Array[Short] => util.Arrays.sort(a)
      case a: Array[Char] => util.Arrays.sort(a)
      case a: Array[Byte] => util.Arrays.sort(a)
      case a: Array[Object] => util.Arrays.sort(a)
    }
    copyArray
  }
}
