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
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream._
import org.scalatest.funsuite.AnyFunSuite

class SampleStreamingActionTest extends AnyFunSuite with StreamingActionBase {

  test("a simple action") {
    val input = List(List("hi"), List("bye"))
    val acc = sc.accumulator(0)
    val cw = countWordsLength(acc)
    runAction(input, cw)
    assert(5 === acc.value)
  }

  def countWordsLength(acc: Accumulator[Int]): (DStream[String] => Unit) = {
    def c(input: DStream[String]): Unit = {
      input.foreachRDD{r: RDD[String] =>
        r.foreach{e: String => acc += e.length()}}
    }
    c _
  }
}
