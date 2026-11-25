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
import java.time.Duration

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.internal.SQLConf
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.JsonMethods._
import org.scalactic.source
import org.scalatest.Suite
import org.scalatest.Tag
import org.scalatest.funsuite.AnyFunSuite

import scala.util.control.NonFatal

case class ColumnMismatch(smth: String) extends Exception(smth)

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

  protected def enableHiveSupport: Boolean = true
  protected def enableIcebergSupport: Boolean = false

  lazy val tempDir = Utils.createTempDir()
  lazy val localMetastorePath = new File(tempDir, "metastore").getCanonicalPath
  lazy val localWarehousePath = new File(tempDir, "warehouse").getCanonicalPath
  val icebergWarehouse = new File(tempDir, "iceberg-warehouse").getCanonicalPath

  /**
   * Constructs a configuration for hive or iceberg, where the metastore is located in a
   * temp directory.
   */
  def builder(): org.apache.spark.sql.SparkSession.Builder = {
    // Yes this is using a lot of mutation on the builder.
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
      builder.config("javax.jdo.option.ConnectionURL",
        s"jdbc:derby:;databaseName=$localMetastorePath;create=true")
      builder.config("datanucleus.rdbms.datastoreAdapterClassName",
        "org.datanucleus.store.rdbms.adapter.DerbyAdapter")
    }
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
    if (enableIcebergSupport) {
      builder.config("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      builder.config("spark.sql.catalog.spark_catalog",
        "org.apache.iceberg.spark.SparkSessionCatalog")
      builder.config("spark.sql.catalog.spark_catalog.type",
        "hive")
      builder.config("spark.sql.catalog.local",
        "org.apache.iceberg.spark.SparkCatalog")
      builder.config("spark.sql.catalog.local.type",
        "hadoop")
      builder.config("spark.sql.catalog.local.warehouse",
        icebergWarehouse)
    }
    builder
  }

  def sqlBeforeAllTestCases(): Unit = {
    if ((SparkSessionProvider._sparkSession ne null) &&
      !SparkSessionProvider._sparkSession.sparkContext.isStopped) {
      // Use existing session if its around and running.
    } else {

      SparkSessionProvider._sparkSession = builder.getOrCreate()
    }
  }

  /**
   * Compare if two schemas are equal, ignoring __autoGeneratedAlias magic
   */
  def assertSchemasEqual(expected: StructType, result: StructType): Unit =
    assertSchemasEqual(expected, result, ignoreNullable = false,
      ignoreColumnOrder = false, ignoreColumnName = false)

  /**
   * Compare if two schemas are equal, ignoring __autoGeneratedAlias magic.
   *
   * @param ignoreNullable    ignore nullability, including nested containsNull and
   *                          valueContainsNull.
   * @param ignoreColumnOrder compare top level fields sorted by name.
   * @param ignoreColumnName  compare top level fields by position, ignoring names.
   */
  def assertSchemasEqual(expected: StructType, result: StructType,
                         ignoreNullable: Boolean, ignoreColumnOrder: Boolean,
                         ignoreColumnName: Boolean): Unit = {
    val cleanExpected = SchemaComparison.normalize(
      expected, ignoreNullable, ignoreColumnOrder, ignoreColumnName)
    val cleanResult = SchemaComparison.normalize(
      result, ignoreNullable, ignoreColumnOrder, ignoreColumnName)
    if (cleanExpected != cleanResult) {
      assert(SchemaComparison.describeDifferences(cleanExpected, cleanResult),
        cleanExpected, cleanResult)
    }
  }

  /**
   * Compares if two [[DataFrame]]s are equal, checks the schema and then if that
   * matches checks if the rows are equal.
    *
    * @param customShow unit function to customize the '''show''' method
    *                   when dataframes are not equal. IE: '''df.show(false)''' or
    *                   '''df.toJSON.show(false)'''.
   */
  def assertDataFrameEquals(expected: DataFrame, result: DataFrame,
                            customShow: DataFrame => Unit = _.show()): Unit = {
    assertDataFrameEquals(expected, result, DataFrameEqualityOptions(), customShow)
  }

  /**
   * Compares if two [[DataFrame]]s are equal using the given options, see
   * [[DataFrameEqualityOptions]] for tolerances, ignoring row order, column
   * order, column names, column types and nullability.
   *
   * {{{
   * assertDataFrameEquals(expected, result,
   *   DataFrameEqualityOptions(rtol = 1e-5, checkRowOrder = false))
   * }}}
   */
  def assertDataFrameEquals(expected: DataFrame, result: DataFrame,
                            options: DataFrameEqualityOptions): Unit = {
    assertDataFrameEquals(expected, result, options, (df: DataFrame) => df.show())
  }

  /**
   * Compares if two [[DataFrame]]s are equal using the given options.
   *
   * @param customShow unit function to customize the '''show''' method
   *                   when dataframes are not equal.
   */
  def assertDataFrameEquals(expected: DataFrame, result: DataFrame,
                            options: DataFrameEqualityOptions,
                            customShow: DataFrame => Unit): Unit = {
    val preparedExpected = prepareForComparison(expected, options)
    val preparedResult = prepareForComparison(result, options)
    // Column order and names have already been aligned by prepareForComparison.
    assertSchemasEqual(preparedExpected.schema, preparedResult.schema,
      options.ignoreNullable, ignoreColumnOrder = false, ignoreColumnName = false)

    if (options.checkRowOrder) {
      assertRowsEqualInOrder(preparedExpected, preparedResult, options, customShow)
    } else if (options.isExact) {
      assertDataFrameDataEquals(preparedExpected, preparedResult, maxErrorsToShow(options))
    } else {
      // With tolerances rows can not be matched by grouping, so sort both sides
      // and compare in order. Note: values within tolerance of each other that
      // sort differently relative to other rows may still be mis-paired.
      assertRowsEqualInOrder(sortForComparison(preparedExpected),
        sortForComparison(preparedResult), options, customShow)
    }
  }

  private def maxErrorsToShow(options: DataFrameEqualityOptions): Int =
    options.maxErrors.getOrElse(maxUnequalRowsToShow)

  /**
   * Apply the column level options (order, names, types) to a DataFrame.
   */
  private[testing] def prepareForComparison(
      df: DataFrame, options: DataFrameEqualityOptions): DataFrame = {
    import SchemaComparison.quote
    var prepared = df
    if (options.ignoreColumnOrder && !options.ignoreColumnName) {
      prepared = prepared.select(prepared.columns.sorted.map(c => col(quote(c))): _*)
    }
    if (options.ignoreColumnName) {
      prepared = prepared.toDF(prepared.columns.indices.map(_.toString): _*)
    }
    if (options.ignoreColumnType) {
      prepared = prepared.select(prepared.schema.fields.map { f =>
        val asString = col(quote(f.name)).cast(StringType)
        val normalized = f.dataType match {
          // Drop trailing zeros so 1 (int) and 1.0 (double) compare equal.
          case FloatType | DoubleType | _: DecimalType =>
            regexp_replace(asString, "\\.0+$", "")
          case _ => asString
        }
        normalized.as(f.name)
      }: _*)
    }
    prepared
  }

  /** Sort by every sortable column so two DataFrames can be compared in order. */
  private[testing] def sortForComparison(df: DataFrame): DataFrame = {
    val keys = df.schema.fields
      .filter(f => SchemaComparison.isOrderable(f.dataType))
      .map(f => col(SchemaComparison.quote(f.name)).asc_nulls_first)
    if (keys.isEmpty) df else df.sort(keys: _*)
  }

  /**
   * Compare two DataFrames row by row, in order. The default implementation
   * uses RDDs so it scales to large DataFrames; Spark Connect overrides this
   * with a collect based implementation.
   */
  protected def compareRowsInOrder(expected: DataFrame, result: DataFrame,
                                   options: DataFrameEqualityOptions,
                                   maxRows: Int): RowComparison.OrderedComparison = {
    import RowComparison.OrderedComparison
    val expectedRDD = expected.rdd
    val resultRDD = result.rdd
    try {
      expectedRDD.cache()
      resultRDD.cache()
      val expectedCount = expectedRDD.count()
      val resultCount = resultRDD.count()
      if (expectedCount != resultCount) {
        OrderedComparison(expectedCount, resultCount, 0, Seq.empty)
      } else {
        val (tol, rtol, tolTimestamp) = (options.tol, options.rtol, options.tolTimestamp)
        val unequalRDD = zipWithIndex(expectedRDD).join(zipWithIndex(resultRDD))
          .filter { case (_, (r1, r2)) =>
            !RowComparison.compareRows(r1, r2, tol, rtol, tolTimestamp)
          }
        val sample = unequalRDD
          .takeOrdered(maxRows)(Ordering.by[(Long, (Row, Row)), Long](_._1))
          .map { case (idx, (r1, r2)) => (idx, r1, r2) }
        val numUnequal = if (sample.isEmpty) 0L else unequalRDD.count()
        OrderedComparison(expectedCount, resultCount, numUnequal, sample)
      }
    } finally {
      expectedRDD.unpersist()
      resultRDD.unpersist()
    }
  }

  private def assertRowsEqualInOrder(expected: DataFrame, result: DataFrame,
                                     options: DataFrameEqualityOptions,
                                     customShow: DataFrame => Unit): Unit = {
    import scala.collection.JavaConverters._

    val comparison = compareRowsInOrder(expected, result, options, maxErrorsToShow(options))
    assert(s"Length not Equal: expected ${comparison.expectedCount} rows, " +
      s"result has ${comparison.resultCount} rows",
      comparison.expectedCount, comparison.resultCount)

    if (comparison.sample.nonEmpty) {
      val unequalSchema = StructType(
        StructField("source_dataframe", StringType) :: expected.schema.fields.toList)
      try {
        val df = spark.createDataFrame(
          comparison.sample.flatMap { case (_, r1, r2) =>
            Seq(tagRow(r1, "expected", unequalSchema), tagRow(r2, "result", unequalSchema))
          }.toList.asJava, unequalSchema)
        customShow(df)
      } catch {
        // Showing the rows is best effort, never hide the real failure.
        case NonFatal(_) =>
      }
      val diffs = comparison.sample.map { case (idx, r1, r2) =>
        RowComparison.RowDiff(idx, Some(r1), Some(r2))
      }
      fail("There are some unequal rows\n" + RowComparison.formatDiff(
        diffs, comparison.numUnequal, comparison.expectedCount,
        expected.schema.fieldNames.toSeq, options))
    }
  }

  /**
   * Compare if two columns in the same DataFrame are equal. Nulls are only
   * equal to nulls.
   */
  def assertColumnEquality(
    df: DataFrame,
    colName1: String,
    colName2: String
  ): Unit = {
    val elements = df.select(colName1, colName2)
      .filter(not(col(colName1) <=> col(colName2))).collect()
    val colName1Elements = elements.map(_(0))
    val colName2Elements = elements.map(_(1))
    val mismatchMessage = s"""
Columns aren't equal
'$colName1' elements:
[${colName1Elements.mkString(", ")}]
'$colName2' elements:
[${colName2Elements.mkString(", ")}]
"""
    if (elements.nonEmpty) {
      throw ColumnMismatch(mismatchMessage)
    }
  }

  /**
    * Compares if two [[DataFrame]]s are equal, checks that the schemas are the same.
    * When comparing inexact fields uses tol.
    *
    * @param tol max acceptable tolerance for numeric (between(0, 1)) &
    *            timestamp (millis).
    */
  @deprecated(
    "Use `assertDataFrameApproximateEquals` with timestamp tolerance",
    since = "1.5.0"
  )
  def assertDataFrameApproximateEquals(expected: DataFrame, result: DataFrame,
                                       tol: Double): Unit =
    assertDataFrameApproximateEquals(expected, result, tol,
      Duration.ofMillis(tol.toLong), _.show())

  /**
    * Compares if two [[DataFrame]]s are equal, checks that the schemas are the same.
    * When comparing inexact fields uses tol & tolTimestamp.
    *
    * @param tol          max acceptable numeric tolerance, should be less than 1.
    * @param tolTimestamp max acceptable timestamp tolerance.
    * @param customShow   unit function to customize the '''show''' method
    *                     when dataframes are not equal. IE: '''df.show(false)''' or
    *                     '''df.toJSON.show(false)'''.
    */
  def assertDataFrameApproximateEquals(
    expected: DataFrame, result: DataFrame,
    tol: Double, tolTimestamp: Duration,
    customShow: DataFrame => Unit = _.show()): Unit = {
    assertDataFrameEquals(expected, result,
      DataFrameEqualityOptions(tol = tol, tolTimestamp = tolTimestamp), customShow)
  }

  private[testing] def tagRow(
    row: Row, tag: String, schema: StructType): Row =
    new GenericRowWithSchema((tag +: row.toSeq).toArray, schema)


  /**
    * Compares if two [[DataFrame]]s are equal without caring about order of rows, by
    * finding elements in one DataFrame that is not in the other. The resulting
    * DataFrame should be empty inferring the two DataFrames have the same elements.
    * Also verifies that the schema is identical.
    */
  def assertDataFrameNoOrderEquals(expected: DataFrame, result: DataFrame): Unit = {
    assertDataFrameEquals(expected, result, DataFrameEqualityOptions(checkRowOrder = false))
  }


  /**
   * Compares if two [[DataFrame]]s are equal without caring about order of rows, by
   * finding elements in one DataFrame that is not in the other. The resulting
   * DataFrame should be empty inferring the two DataFrames have the same elements.
   * Does not compare the schema.
   */
  def assertDataFrameDataEquals(expected: DataFrame, result: DataFrame): Unit =
    assertDataFrameDataEquals(expected, result, maxUnequalRowsToShow)

  private def assertDataFrameDataEquals(expected: DataFrame, result: DataFrame,
                                        maxErrors: Int): Unit = {
    import SchemaComparison.quote
    val expectedCol = "assertDataFrameNoOrderEquals_expected"
    val actualCol = "assertDataFrameNoOrderEquals_actual"
    val expectedPostMap = convertMapToArrayStruct(expected)
    val resultPostMap = convertMapToArrayStruct(result)
    try {
      expected.cache()
      result.cache()
      assert("Column size not Equal", expected.columns.size, result.columns.size)
      val expectedCount = expected.count()
      val resultCount = result.count()
      assert(s"Length not Equal: expected $expectedCount rows, result has $resultCount rows",
        expectedCount, resultCount)

      val columns = expectedPostMap.columns.map(s => col(quote(s)))
      val expectedElementsCount = expectedPostMap
        .groupBy(columns: _*)
        .agg(count(lit(1)).as(expectedCol))
      val resultElementsCount = resultPostMap
        .groupBy(columns: _*)
        .agg(count(lit(1)).as(actualCol))

      val joinExprs = expectedPostMap.columns
        .map(s => expectedPostMap.col(quote(s)) <=> resultPostMap.col(quote(s)))
        .reduce(_.and(_))
      val diff = expectedElementsCount
        .join(resultElementsCount, joinExprs, "full_outer")
        .filter(not(col(expectedCol) <=> col(actualCol)))

      val diffRows = diff.take(maxErrors)
      if (diffRows.nonEmpty) {
        val numColumns = expectedPostMap.columns.length
        val fieldNames = expectedPostMap.columns.toSeq
        val described = diffRows.map { row =>
          val values = row.toSeq
          val expectedOccurrences = Option(values(numColumns)).getOrElse(0L)
          val actualOccurrences = Option(values(2 * numColumns + 1)).getOrElse(0L)
          val rowValues =
            if (row.isNullAt(numColumns)) values.slice(numColumns + 1, 2 * numColumns + 1)
            else values.take(numColumns)
          val rendered = fieldNames.zip(rowValues).map { case (name, v) =>
            s"$name=${RowComparison.render(v)}"
          }.mkString("[", ", ", "]")
          s"$rendered: expected $expectedOccurrences time(s), " +
            s"found $actualOccurrences time(s)"
        }
        fail("DataFrames contain different rows (ignoring order), " +
          s"showing up to $maxErrors:\n" + described.mkString("\n"))
      }
    } finally {
      expected.unpersist()
      result.unpersist()
    }
  }

  /**
    * Converts map-typed columns into array-of-struct columns so that [[DataFrame]]s
    * containing maps can be compared reliably in tests.
    *
    * Spark does not define a stable ordering for values of map type, which means that
    * operations used in test assertions (such as set-like comparisons that ignore
    * row order) cannot directly rely on map columns being orderable. This helper
    * normalizes map columns into an orderable representation before comparison.
    *
    * For each map column, this method produces an array of structs created by
    * zipping the map keys and values via `arrays_zip(map_keys(col), map_values(col))`.
    * In the resulting struct, field `"0"` holds the key and field `"1"` holds the
    * corresponding value. Non-map columns are preserved as-is.
    *
    * The transformation is applied recursively to nested structures: maps inside
    * structs, arrays of structs containing maps, and other combinations are all
    * rewritten so that every map encountered in the schema is converted to an
    * array of structs in this way.
    *
    * The method is marked `private[testing]` because it is an internal utility of
    * the testing framework, intended to support DataFrame equality checks rather
    * than to be part of the public API.
    */
  private[testing] def convertMapToArrayStruct(df: DataFrame): DataFrame = {
    import SchemaComparison.quote
    def buildExpr(dataType: DataType, colName: String): String = dataType match {
      case MapType(_, _, _) =>
        s"arrays_zip(map_keys($colName), map_values($colName)) AS $colName"
      case StructType(fields) =>
        val inner = fields.map { f =>
          s"'${f.name}', ${buildExpr(f.dataType, s"$colName.${quote(f.name)}").replaceAll(" AS .*", "")}"
        }.mkString(", ")
        s"named_struct($inner) AS $colName"
      case ArrayType(elementType, _) =>
        elementType match {
          case MapType(_, _, _) =>
            val mapExpr =
              s"arrays_zip(map_keys(x), map_values(x))"
            s"flatten(transform($colName, x -> $mapExpr)) AS $colName"
          case StructType(fields) =>
            val structExpr =
              fields.map(f =>
                s"'${f.name}', ${buildExpr(f.dataType, s"x.${quote(f.name)}").replaceAll(" AS .*", "")}"
              ).mkString(", ")
            s"transform($colName, x -> named_struct($structExpr)) AS $colName"
          case _ =>
            val inner = buildExpr(elementType, "x").replaceAll(" AS .*", "")
            s"transform($colName, x -> $inner) AS $colName"
        }
      case _ =>
        s"$colName"
    }

    val exprs = df.schema.fields.map(f => buildExpr(f.dataType, quote(f.name)))
    df.selectExpr(exprs: _*)
  }

  /**
   * Compares if two [[DataFrame]]s are equal without caring about order of rows, by
   * finding elements in one DataFrame that is not in the other.
   * Similar to the function assertDataFrameDataEquals but for small [[DataFrame]]
   * that can be collected in memory for the comparison.
   */
  def assertSmallDataFrameDataEquals(
    expected: DataFrame, result: DataFrame): Unit = {

    val cols = expected.columns
    assert("Column size not Equal", cols.size, result.columns.size)
    assertTrue(expected.collect.toSet ==
      result.select(cols.head, cols.tail: _*).collect.toSet)
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
    DataFrameSuiteBase.approxEquals(r1, r2, tol, Duration.ofMillis(tol.toLong))
  }

  def approxEquals(r1: Row, r2: Row, tolTimestamp: Duration): Boolean = {
    DataFrameSuiteBase.approxEquals(r1, r2, tolTimestamp)
  }

  def approxEquals(r1: Row, r2: Row, tol: Double,
                   tolTimestamp: Duration): Boolean = {
    DataFrameSuiteBase.approxEquals(r1, r2, tol, tolTimestamp)
  }
}

object DataFrameSuiteBase {

  /** Approximate equality, based on equals from [[Row]] */
  def approxEquals(r1: Row, r2: Row, tol: Double): Boolean =
    approxEquals(r1, r2, tol, Duration.ofNanos((tol*1000).toLong))

  /** Approximate equality, based on equals from [[Row]] */
  def approxEquals(r1: Row, r2: Row, tolTimestamp: Duration): Boolean =
    approxEquals(r1, r2, 0, tolTimestamp)

  /**
   * Approximate equality, based on equals from [[Row]].
   * See [[RowComparison.compareValues]] for how each type is compared.
   */
  def approxEquals(r1: Row, r2: Row, tol: Double,
                   tolTimestamp: Duration): Boolean =
    RowComparison.compareRows(r1, r2, tol, 0.0, tolTimestamp)
}

object SparkSessionProvider {
  @transient var _sparkSession: SparkSession = _
  def sqlContext: SQLContext = EvilSessionTools.extractSQLContext(_sparkSession)
  def sparkSession = _sparkSession
}
