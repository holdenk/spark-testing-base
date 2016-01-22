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

import java.io.File

import scala.math.abs
import scala.collection.mutable.HashMap

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.hive._
import org.apache.spark.sql.types.StructType
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars

import org.scalatest.{FunSuiteLike}

/**
 * :: Experimental ::
 * Base class for testing Spark DataFrames.
 */

trait DataFrameSuiteBase extends DataFrameSuiteBaseLike with SharedSparkContext {
  override def beforeAll() {
    super.beforeAll()
    super.beforeAllTestCases()
  }

  override def afterAll() {
    super.afterAllTestCases()
    super.afterAll()
  }

}

trait DataFrameSuiteBaseLike extends FunSuiteLike with SparkContextProvider with Serializable {
  val maxUnequalRowsToShow = 10
  def sqlContext: HiveContext = SQLContextProvider._sqlContext

  def beforeAllTestCases() {
    /** Constructs a configuration for hive, where the metastore is located in a temp directory. */
    val tempDir = Utils.createTempDir()
    val localMetastorePath = new File(tempDir, "metastore").getCanonicalPath
    val localWarehousePath = new File(tempDir, "wharehouse").getCanonicalPath
    def newTemporaryConfiguration(): Map[String, String] = {
      val propMap: HashMap[String, String] = HashMap()
      // We have to mask all properties in hive-site.xml that relates to metastore data source
      // as we used a local metastore here.
      HiveConf.ConfVars.values().foreach { confvar =>
        if (confvar.varname.contains("datanucleus") || confvar.varname.contains("jdo")) {
          propMap.put(confvar.varname, confvar.getDefaultExpr())
        }
      }
      propMap.put("javax.jdo.option.ConnectionURL",
        s"jdbc:derby:;databaseName=$localMetastorePath;create=true")
      propMap.put("datanucleus.rdbms.datastoreAdapterClassName",
        "org.datanucleus.store.rdbms.adapter.DerbyAdapter")
      propMap.put(ConfVars.METASTOREURIS.varname, "")
      propMap.toMap
    }
    val config = newTemporaryConfiguration()

    SQLContextProvider._sqlContext = new TestHiveContext(sc, config, localMetastorePath, localWarehousePath)
  }

  def afterAllTestCases() {
    SQLContextProvider._sqlContext = null
  }

  /**
   * Compares if two [[DataFrame]]s are equal, checks the schema and then if that matches
   * checks if the rows are equal.
   */
  def equalDataFrames(expected: DataFrame, result: DataFrame) {
    equalSchema(expected.schema, result.schema)

    try {
      expected.rdd.cache
      result.rdd.cache
      assert(expected.rdd.count == result.rdd.count)

      val expectedIndexValue = zipWithIndex(expected.rdd)
      val resultIndexValue = zipWithIndex(result.rdd)

      val unequalRDD = expectedIndexValue.join(resultIndexValue).filter{case (idx, (r1, r2)) =>
        !(r1.equals(r2) || DataFrameSuiteBase.approxEquals(r1, r2, 0.0))}

      assert(unequalRDD.take(maxUnequalRowsToShow).isEmpty)
    } finally {
      expected.rdd.unpersist()
      result.rdd.unpersist()
    }
  }

  /**
    * Compares if two [[DataFrame]]s are equal, checks that the schemas are the same.
    * When comparing inexact fields uses tol.
    *
    * @param tol max acceptable tolerance, should be less than 1.
    */
  def approxEqualDataFrames(expected: DataFrame, result: DataFrame, tol: Double) {
    equalSchema(expected.schema, result.schema)

    try {
      expected.rdd.cache
      result.rdd.cache
      assert(expected.rdd.count == result.rdd.count)

      val expectedIndexValue = zipWithIndex(expected.rdd)
      val resultIndexValue = zipWithIndex(result.rdd)

      val unequalRDD = expectedIndexValue.join(resultIndexValue).filter{case (idx, (r1, r2)) =>
        !DataFrameSuiteBase.approxEquals(r1, r2, tol)}

      assert(unequalRDD.take(maxUnequalRowsToShow).isEmpty)
    } finally {
      expected.rdd.unpersist()
      result.rdd.unpersist()
    }
  }

  /**
    * Zip RDD's with precise indexes. This is used so we can join two DataFrame's
    * Rows together regardless of if the source is different but still compare based on
    * the order.
    */
  private[testing] def zipWithIndex[U](rdd: RDD[U]) = rdd.zipWithIndex().map{ case (row, idx) => (idx, row) }

  /**
   * Compares the schema
   */
  def equalSchema(expected: StructType, result: StructType): Unit = {
    assert(expected.treeString === result.treeString)
  }

  def approxEquals(r1: Row, r2: Row, tol: Double): Boolean = {
    DataFrameSuiteBase.approxEquals(r1, r2, tol)
  }
}

object DataFrameSuiteBase {

  /** Approximate equality, based on equals from [[Row]] */
  def approxEquals(r1: Row, r2: Row, tol: Double): Boolean = {
    if (r1.length != r2.length) {
      return false
    } else {
      var idx = 0
      val length = r1.length
      while (idx < length) {
        if (r1.isNullAt(idx) != r2.isNullAt(idx))
          return false

        if (!r1.isNullAt(idx)) {
          val o1 = r1.get(idx)
          val o2 = r2.get(idx)
          o1 match {
            case b1: Array[Byte] =>
              if (!java.util.Arrays.equals(b1, o2.asInstanceOf[Array[Byte]])) return false

            case f1: Float =>
              if (java.lang.Float.isNaN(f1) != java.lang.Float.isNaN(o2.asInstanceOf[Float])) return false
              if (abs(f1 - o2.asInstanceOf[Float]) > tol) return false

            case d1: Double =>
              if (java.lang.Double.isNaN(d1) != java.lang.Double.isNaN(o2.asInstanceOf[Double])) return false
              if (abs(d1 - o2.asInstanceOf[Double]) > tol) return false

            case d1: java.math.BigDecimal =>
              if (d1.compareTo(o2.asInstanceOf[java.math.BigDecimal]) != 0) return false

            case _ =>
              if (o1 != o2) return false
          }
        }
        idx += 1
      }
    }
    true
  }
}

object SQLContextProvider {
    @transient var _sqlContext: HiveContext = _
}
