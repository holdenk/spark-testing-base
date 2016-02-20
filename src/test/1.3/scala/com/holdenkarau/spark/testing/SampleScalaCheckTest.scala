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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StringType, StructType, IntegerType, StructField}
import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Prop.forAll
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

class SampleScalaCheckTest extends FunSuite with SharedSparkContext with Checkers {
  // tag::propertySample[]
  // A trivial property that the map doesn't change the number of elements
  test("map should not change number of elements") {
    val property =
      forAll(RDDGenerator.genRDD[String](sc)(Arbitrary.arbitrary[String])){
        rdd => rdd.map(_.length).count() == rdd.count()
      }

    check(property)
  }
  // end::propertySample[]
  // A slightly more complex property check using RDDComparisions
  // tag::propertySample2[]
  test("assert that two methods on the RDD have the same results") {
    val property =
      forAll(RDDGenerator.genRDD[String](sc)(Arbitrary.arbitrary[String])) {
        rdd => RDDComparisons.compare(filterOne(rdd), filterOther(rdd)).isEmpty
      }

    check(property)
  }
  // end::propertySample2[]

//  test("assert generating rows correctly") {
//    val schema = StructType(List(StructField("name", StringType), StructField("age", IntegerType)))
//    val rowGen: Arbitrary[Row] = DataframeGenerator.getRowGenerator(schema)
//    println("Sample: " + rowGen.arbitrary.sample)
//
//    forAll(rowGen.arbitrary) {
//      row => row.schema === schema && row.get(0).isInstanceOf[String] && row.get(1).isInstanceOf[Int]
//    }
//  }

//  test("assert generating dataframes") {
//    val schema = StructType(List(StructField("name", StringType), StructField("age", IntegerType)))
//    val dataframeGen: Arbitrary[DataFrame] = DataframeGenerator.genDataFrame(sc, schema)
//    println("Sample:")
//    dataframeGen.arbitrary.sample.get.show
//
//    forAll(dataframeGen.arbitrary) {
//      dataframe => dataframe.schema === schema
//    }
//  }

  def filterOne(rdd: RDD[String]): RDD[Int] = {
    rdd.filter(_.length > 2).map(_.length)
  }
  def filterOther(rdd: RDD[String]): RDD[Int] = {
    rdd.map(_.length).filter(_ > 2)
  }
}
