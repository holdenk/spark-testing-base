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
import java.sql.Timestamp

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.scalactic.source
import org.scalatest.Suite
import org.scalatest.Tag
import org.scalatest.funsuite.AnyFunSuite

import scala.math.abs

/**
 * Base trait for testing Spark DataFrames in Scala.
 */
trait ScalaDataFrameSuiteBase extends AnyFunSuite with DataFrameSuiteBase {
  /*
   * If you need test your function with both codegen and non-codegen paths. This should be relatively
   * rare unless you are writing your own Spark expressions (w/ custom codegen).
   * This is taken from the "test" function inside of the PlanTest trait in SparkSQL.
   */
  def testCombined(
      testName: String,
      testTags: Tag*)(testFun: => Any)(implicit pos: source.Position): Unit = {
    System.setProperty("SPARK_TESTING", "yes") // codegen modes are not always respected
    val codegenMode = CodegenObjectFactoryMode.CODEGEN_ONLY.toString
    val interpretedMode = CodegenObjectFactoryMode.NO_CODEGEN.toString

    test(testName + " (codegen path)", testTags: _*)(
      withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegenMode) { testFun })(pos)
    test(testName + " (interpreted path)", testTags: _*)(
      withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> interpretedMode,
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
        SQLConf.WHOLESTAGE_MAX_NUM_FIELDS.key -> "0",
        SQLConf.WHOLESTAGE_HUGE_METHOD_LIMIT.key -> "0"
      ) { testFun })(pos)
  }
  def testCodegenOnly(
      testName: String,
      testTags: Tag*)(testFun: => Any)(implicit pos: source.Position): Unit = {
    System.setProperty("SPARK_TESTING", "yes") // codegen modes are not always respected
    val codegenMode = CodegenObjectFactoryMode.CODEGEN_ONLY.toString

    test(testName + " (codegen path)", testTags: _*)(
      withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegenMode) { testFun })(pos)
  }
  def testNonCodegen(
      testName: String,
      testTags: Tag*)(testFun: => Any)(implicit pos: source.Position): Unit = {
    System.setProperty("SPARK_TESTING", "yes") // codegen modes are not always respected
    val interpretedMode = CodegenObjectFactoryMode.NO_CODEGEN.toString

    test(testName + " (interpreted path)", testTags: _*)(
      withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> interpretedMode,
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
        SQLConf.WHOLESTAGE_MAX_NUM_FIELDS.key -> "1",
        SQLConf.WHOLESTAGE_HUGE_METHOD_LIMIT.key -> "1"
      ) { testFun })(pos)
  }
}

/**
 * :: Experimental ::
 * Base class for testing Spark DataFrames.
 */
trait DataFrameSuiteBase extends TestSuite
    with SharedSparkContext with DataFrameSuiteBaseLike { self: Suite =>
  override def beforeAll(): Unit = {
    super.beforeAll()
    super.sqlBeforeAllTestCases()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    if (!reuseContextIfPossible) {
      if (spark != null) {
        spark.stop()
      }
      if (sc != null) {
        sc.stop()
      }
      SparkSessionProvider._sparkSession = null
    }
  }

  /**
   * Sets all SQL configurations specified in `pairs`, calls `f`, and then restores all SQL
   * configurations.
   * Taken from Spark SQLHelper.
   */
  protected def withSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
    val currentConf = spark.sessionState.conf
    val (keys, values) = pairs.unzip
    val currentValues = keys.map { key =>
      if (currentConf.contains(key)) {
        Some(currentConf.getConfString(key))
      } else {
        None
      }
    }
    (keys, values).zipped.foreach { (k: String, v: String) =>
      spark.sessionState.conf.setConfString(k, v)
    }
    try f finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => spark.sessionState.conf.setConfString(key, value)
        case (key, None) => spark.sessionState.conf.unsetConf(key)
      }
    }
  }
}

trait DataFrameSuiteBaseLike extends SparkContextProvider
    with TestSuiteLike with Serializable {
  val maxUnequalRowsToShow = 10
  @transient lazy val spark: SparkSession = SparkSessionProvider._sparkSession
  @transient lazy val sqlContext: SQLContext = SparkSessionProvider.sqlContext

  protected implicit def impSqlContext: SQLContext = sqlContext

  protected implicit def enableHiveSupport: Boolean = true

  def sqlBeforeAllTestCases(): Unit = {
    if ((SparkSessionProvider._sparkSession ne null) &&
      !SparkSessionProvider._sparkSession.sparkContext.isStopped) {
      // Use existing session if its around and running.
    } else {
      /**
       * Constructs a configuration for hive, where the metastore is located in a
       * temp directory.
       */
      val tempDir = Utils.createTempDir()
      val localMetastorePath = new File(tempDir, "metastore").getCanonicalPath
      val localWarehousePath = new File(tempDir, "warehouse").getCanonicalPath
      def newBuilder() = {
        val builder = SparkSession.builder()
        // Long story with lz4 issues in 2.3+
        builder.config("spark.io.compression.codec", "snappy")
        // We have to mask all properties in hive-site.xml that relates to metastore
        // data source as we used a local metastore here.
        if (enableHiveSupport) {
          import org.apache.hadoop.hive.conf.HiveConf
          val hiveConfVars = HiveConf.ConfVars.values()
          val accessiableHiveConfVars = hiveConfVars.map(WrappedConfVar(_))
          accessiableHiveConfVars.foreach { confvar =>
            if (confvar.varname.contains("datanucleus") ||
              confvar.varname.contains("jdo")) {
              builder.config(confvar.varname, confvar.getDefaultExpr())
            }
          }
          builder.config(HiveConf.ConfVars.METASTOREURIS.varname, "")
        }
        builder.config("javax.jdo.option.ConnectionURL",
          s"jdbc:derby:;databaseName=$localMetastorePath;create=true")
        builder.config("datanucleus.rdbms.datastoreAdapterClassName",
          "org.datanucleus.store.rdbms.adapter.DerbyAdapter")
        builder.config("spark.sql.streaming.checkpointLocation",
          Utils.createTempDir().toPath().toString)
        builder.config("spark.sql.warehouse.dir",
          localWarehousePath)
        // Enable hive support if available
        try {
          if (enableHiveSupport) {
            builder.enableHiveSupport()
          }
        } catch {
          // Exception is thrown in Spark if hive is not present
          case e: IllegalArgumentException =>
        }
        builder
      }

      val builder = newBuilder()

      SparkSessionProvider._sparkSession = builder.getOrCreate()
    }
  }

  /**
   * Compares if two [[DataFrame]]s are equal, checks the schema and then if that
   * matches checks if the rows are equal.
   */
  def assertDataFrameEquals(expected: DataFrame, result: DataFrame): Unit = {
    assertDataFrameApproximateEquals(expected, result, 0.0)
  }

  /**
    * Compares if two [[DataFrame]]s are equal, checks that the schemas are the same.
    * When comparing inexact fields uses tol.
    *
    * @param tol max acceptable tolerance, should be less than 1.
    */
  def assertDataFrameApproximateEquals(
    expected: DataFrame, result: DataFrame, tol: Double): Unit = {

    assert(expected.schema, result.schema)

    try {
      expected.rdd.cache
      result.rdd.cache
      assert("Length not Equal", expected.rdd.count, result.rdd.count)

      val expectedIndexValue = zipWithIndex(expected.rdd)
      val resultIndexValue = zipWithIndex(result.rdd)

      val unequalRDD = expectedIndexValue.join(resultIndexValue).
        filter{case (idx, (r1, r2)) =>
          !(r1.equals(r2) || DataFrameSuiteBase.approxEquals(r1, r2, tol))}

      assertEmpty(unequalRDD.take(maxUnequalRowsToShow))
    } finally {
      expected.rdd.unpersist()
      result.rdd.unpersist()
    }
  }

  /**
    * Compares if two [[DataFrame]]s are equal without caring about order of rows, by
    * finding elements in one DataFrame that is not in the other. The resulting
    * DataFrame should be empty inferring the two DataFrames have the same elements.
    * Also verifies that the schema is identical.
    */
  def assertDataFrameNoOrderEquals(expected: DataFrame, result: DataFrame) {
    assert(expected.schema, result.schema)
    assertDataFrameDataEquals(expected, result)
  }


  /**
   * Compares if two [[DataFrame]]s are equal without caring about order of rows, by
   * finding elements in one DataFrame that is not in the other. The resulting
   * DataFrame should be empty inferring the two DataFrames have the same elements.
   * Does not compare the schema.
   */
  def assertDataFrameDataEquals(expected: DataFrame, result: DataFrame): Unit = {
    val expectedCol = "assertDataFrameNoOrderEquals_expected"
    val actualCol = "assertDataFrameNoOrderEquals_actual"
    try {
      expected.rdd.cache
      result.rdd.cache
      assert("Column size not Equal", expected.columns.size, result.columns.size)
      assert("Length not Equal", expected.rdd.count, result.rdd.count)

      val columns = expected.columns.map(s => col(s))
      val expectedElementsCount = expected
        .groupBy(columns: _*)
        .agg(count(lit(1)).as(expectedCol))
      val resultElementsCount = result
        .groupBy(columns: _*)
        .agg(count(lit(1)).as(actualCol))

      val joinExprs = expected.columns
        .map(s => expected.col(s) <=> result.col(s)).reduce(_.and(_))
      val diff = expectedElementsCount
        .join(resultElementsCount, joinExprs, "full_outer")
        .filter(not(col(expectedCol) <=> col(actualCol)))

      assertEmpty(diff.take(maxUnequalRowsToShow))
    } finally {
      expected.rdd.unpersist()
      result.rdd.unpersist()
    }
  }

  /**
   * Zip RDD's with precise indexes. This is used so we can join two DataFrame's
   * Rows together regardless of if the source is different but still compare
   * based on the order.
   */
  private[testing] def zipWithIndex[U](rdd: RDD[U]) = {
    rdd.zipWithIndex().map{ case (row, idx) => (idx, row) }
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
      (0 until r1.length).foreach(idx => {
        if (r1.isNullAt(idx) != r2.isNullAt(idx)) {
          return false
        }

        if (!r1.isNullAt(idx)) {
          val o1 = r1.get(idx)
          val o2 = r2.get(idx)
          o1 match {
            case b1: Array[Byte] =>
              if (!java.util.Arrays.equals(b1, o2.asInstanceOf[Array[Byte]])) {
                return false
              }

            case f1: Float =>
              if (java.lang.Float.isNaN(f1) !=
                java.lang.Float.isNaN(o2.asInstanceOf[Float]))
              {
                return false
              }
              if (abs(f1 - o2.asInstanceOf[Float]) > tol) {
                return false
              }

            case d1: Double =>
              if (java.lang.Double.isNaN(d1) !=
                java.lang.Double.isNaN(o2.asInstanceOf[Double]))
              {
                return false
              }
              if (abs(d1 - o2.asInstanceOf[Double]) > tol) {
                return false
              }

            case d1: java.math.BigDecimal =>
              if (d1.compareTo(o2.asInstanceOf[java.math.BigDecimal]) != 0) {
                return false
              }

            case t1: Timestamp =>
              if (abs(t1.getTime - o2.asInstanceOf[Timestamp].getTime) > tol) {
                return false
              }

            case _ =>
              if (o1 != o2) return false
          }
        }
      })
    }
    true
  }
}

object SparkSessionProvider {
  @transient var _sparkSession: SparkSession = _
  def sqlContext: SQLContext = EvilSessionTools.extractSQLContext(_sparkSession)
  def sparkSession = _sparkSession
}
