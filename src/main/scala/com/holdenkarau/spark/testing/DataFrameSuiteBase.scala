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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import org.scalatest.FunSuite

/**
 * :: Experimental ::
 * Base class for testing Spark DataFrames.
 */
trait DataFrameSuiteBase extends FunSuite {
  /**
   * Compares if two [[DataFrame]]s are equal, checks the schema and then if that matches
   * checks if the rows are equal.
   */
  def equalDataFrames(expected: DataFrame, result: DataFrame) {
    equalSchema(expected.schema, result.schema)
  }

  /**
   * Compares if two [[DataFrame]]s are equal, checks that the schemas are the same.
   * When comparing inexact fields uses tol.
   */
  def approxEqualDataFrames(expected: DataFrame, result: DataFrame, tol: Double) {
    equalSchema(expected.schema, result.schema)
  }

  /**
   * Compares the schema
   */
  def equalSchema(expected: StructType, result: StructType) = {
    assert(expected.treeString === result.treeString)
  }
}
