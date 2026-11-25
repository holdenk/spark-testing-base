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

import java.sql.Timestamp
import java.time.{Duration, Instant, LocalDateTime}

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.json4s._
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization

import scala.math.abs

/**
 * Options controlling how two DataFrames are compared by
 * `assertDataFrameEquals(expected, result, options)`.
 *
 * The defaults match the historical spark-testing-base behaviour (row order
 * matters, schemas must match exactly, values must match exactly).
 *
 * @param tol               absolute tolerance for float, double and decimal values.
 * @param rtol              relative tolerance for float, double and decimal values,
 *                          relative to the expected value. Two finite values are equal
 *                          when `|expected - actual| <= tol + rtol * |expected|`.
 * @param tolTimestamp      tolerance for timestamp, Instant and LocalDateTime values.
 * @param checkRowOrder     when false, rows are compared ignoring their order.
 * @param ignoreNullable    ignore nullability (including nested containsNull and
 *                          valueContainsNull) when comparing schemas.
 * @param ignoreColumnOrder match columns by name rather than by position.
 * @param ignoreColumnName  match columns by position, ignoring their names.
 * @param ignoreColumnType  compare the string representation of every column, so
 *                          e.g. an int column of 1 equals a double column of 1.0.
 * @param maxErrors         maximum number of differing rows to report, defaults to
 *                          `maxUnequalRowsToShow`.
 * @param showOnlyDiff      only show the differing fields of each differing row in
 *                          the failure message.
 *
 * Non-finite floating point values never use the tolerances: NaN is only equal
 * to NaN, and an infinity is only equal to an infinity of the same sign.
 */
case class DataFrameEqualityOptions(
    tol: Double = 0.0,
    rtol: Double = 0.0,
    tolTimestamp: Duration = Duration.ZERO,
    checkRowOrder: Boolean = true,
    ignoreNullable: Boolean = false,
    ignoreColumnOrder: Boolean = false,
    ignoreColumnName: Boolean = false,
    ignoreColumnType: Boolean = false,
    maxErrors: Option[Int] = None,
    showOnlyDiff: Boolean = false) {

  // Builder style helpers, mostly for Java users who can not use named arguments.
  def withTol(tol: Double): DataFrameEqualityOptions = copy(tol = tol)
  def withRtol(rtol: Double): DataFrameEqualityOptions = copy(rtol = rtol)
  def withTolTimestamp(tolTimestamp: Duration): DataFrameEqualityOptions =
    copy(tolTimestamp = tolTimestamp)
  def withCheckRowOrder(checkRowOrder: Boolean): DataFrameEqualityOptions =
    copy(checkRowOrder = checkRowOrder)
  def withIgnoreNullable(ignoreNullable: Boolean): DataFrameEqualityOptions =
    copy(ignoreNullable = ignoreNullable)
  def withIgnoreColumnOrder(ignoreColumnOrder: Boolean): DataFrameEqualityOptions =
    copy(ignoreColumnOrder = ignoreColumnOrder)
  def withIgnoreColumnName(ignoreColumnName: Boolean): DataFrameEqualityOptions =
    copy(ignoreColumnName = ignoreColumnName)
  def withIgnoreColumnType(ignoreColumnType: Boolean): DataFrameEqualityOptions =
    copy(ignoreColumnType = ignoreColumnType)
  def withMaxErrors(maxErrors: Int): DataFrameEqualityOptions =
    copy(maxErrors = Some(maxErrors))
  def withShowOnlyDiff(showOnlyDiff: Boolean): DataFrameEqualityOptions =
    copy(showOnlyDiff = showOnlyDiff)

  /** True when values must match exactly (no numeric or temporal tolerance). */
  def isExact: Boolean = tol == 0.0 && rtol == 0.0 && tolTimestamp.isZero
}

object DataFrameEqualityOptions {
  /** The default options, convenient from Java. */
  def defaults(): DataFrameEqualityOptions = DataFrameEqualityOptions()
}

/**
 * Value and row level comparison used by the DataFrame / Dataset assertions.
 * Does not depend on a SparkSession so it can be used (and tested) on its own.
 */
object RowComparison {

  /**
   * Result of comparing two DataFrames row by row in order.
   *
   * @param expectedCount number of rows in expected.
   * @param resultCount   number of rows in result.
   * @param numUnequal    number of differing rows (only meaningful if counts match).
   * @param sample        a sample of (index, expected, result) differing rows.
   */
  case class OrderedComparison(expectedCount: Long, resultCount: Long,
                               numUnequal: Long, sample: Seq[(Long, Row, Row)])

  /** A pair of differing rows, either side may be missing (e.g. extra rows). */
  case class RowDiff(index: Long, expected: Option[Row], actual: Option[Row])

  def compareDouble(expected: Double, actual: Double, tol: Double, rtol: Double): Boolean = {
    if (java.lang.Double.isNaN(expected) || java.lang.Double.isNaN(actual)) {
      java.lang.Double.isNaN(expected) && java.lang.Double.isNaN(actual)
    } else if (java.lang.Double.isInfinite(expected) || java.lang.Double.isInfinite(actual)) {
      expected == actual
    } else {
      abs(expected - actual) <= tol + rtol * abs(expected)
    }
  }

  def compareDecimal(expected: java.math.BigDecimal, actual: java.math.BigDecimal,
                     tol: Double, rtol: Double): Boolean = {
    if (expected.compareTo(actual) == 0) {
      true
    } else if (java.lang.Double.isNaN(tol) || java.lang.Double.isInfinite(tol) ||
               java.lang.Double.isNaN(rtol) || java.lang.Double.isInfinite(rtol)) {
      false
    } else {
      val allowed = new java.math.BigDecimal(tol)
        .add(new java.math.BigDecimal(rtol).multiply(expected.abs))
      expected.subtract(actual).abs.compareTo(allowed) <= 0
    }
  }

  private def withinDuration(between: Duration, tolTimestamp: Duration): Boolean =
    between.abs.compareTo(tolTimestamp) <= 0

  /**
   * Compare two values, recursing into rows (structs), sequences (arrays) and
   * maps. Never throws on mismatched types, it returns false instead.
   */
  def compareValues(expected: Any, actual: Any, tol: Double, rtol: Double,
                    tolTimestamp: Duration): Boolean = (expected, actual) match {
    case (null, null) => true
    case (null, _) | (_, null) => false
    case (e: Array[Byte], a: Array[Byte]) => java.util.Arrays.equals(e, a)
    case (e: Double, a: Double) => compareDouble(e, a, tol, rtol)
    // Widen floats before subtracting so the difference is not rounded to float precision.
    case (e: Float, a: Float) => compareDouble(e.toDouble, a.toDouble, tol, rtol)
    case (e: java.math.BigDecimal, a: java.math.BigDecimal) => compareDecimal(e, a, tol, rtol)
    case (e: scala.math.BigDecimal, a: scala.math.BigDecimal) =>
      compareDecimal(e.bigDecimal, a.bigDecimal, tol, rtol)
    case (e: Timestamp, a: Timestamp) =>
      withinDuration(Duration.between(e.toInstant, a.toInstant), tolTimestamp)
    case (e: Instant, a: Instant) => withinDuration(Duration.between(e, a), tolTimestamp)
    case (e: LocalDateTime, a: LocalDateTime) =>
      withinDuration(Duration.between(e, a), tolTimestamp)
    case (e: Row, a: Row) => compareRows(e, a, tol, rtol, tolTimestamp)
    case (e: scala.collection.Seq[_], a: scala.collection.Seq[_]) =>
      compareSeqs(e, a, tol, rtol, tolTimestamp)
    case (e: Array[_], a: Array[_]) =>
      compareSeqs(e.toSeq, a.toSeq, tol, rtol, tolTimestamp)
    case (e: scala.collection.Map[_, _], a: scala.collection.Map[_, _]) =>
      val am = a.asInstanceOf[scala.collection.Map[Any, Any]]
      e.size == a.size && e.forall { case (k, v) =>
        am.get(k) match {
          case Some(v2) => compareValues(v, v2, tol, rtol, tolTimestamp)
          case None => false
        }
      }
    case _ => expected == actual
  }

  private def compareSeqs(e: scala.collection.Seq[_], a: scala.collection.Seq[_],
                          tol: Double, rtol: Double, tolTimestamp: Duration): Boolean =
    e.length == a.length && e.iterator.zip(a.iterator).forall { case (x, y) =>
      compareValues(x, y, tol, rtol, tolTimestamp)
    }

  /** Compare two rows field by field. Rows of different length are never equal. */
  def compareRows(expected: Row, actual: Row, tol: Double, rtol: Double,
                  tolTimestamp: Duration): Boolean = {
    if (expected == null || actual == null) {
      expected == null && actual == null
    } else {
      expected.length == actual.length &&
        (0 until expected.length).forall { idx =>
          compareValues(expected.get(idx), actual.get(idx), tol, rtol, tolTimestamp)
        }
    }
  }

  def compareRows(expected: Row, actual: Row, options: DataFrameEqualityOptions): Boolean =
    compareRows(expected, actual, options.tol, options.rtol, options.tolTimestamp)

  /** Render a value for a failure message, showing array contents rather than refs. */
  private[testing] def render(value: Any): String = value match {
    case null => "null"
    case b: Array[Byte] => b.map("%02X".format(_)).mkString("[", " ", "]")
    case a: Array[_] => a.map(render).mkString("[", ",", "]")
    case r: Row => r.toSeq.map(render).mkString("[", ",", "]")
    case s: scala.collection.Seq[_] => s.map(render).mkString("[", ",", "]")
    case m: scala.collection.Map[_, _] =>
      m.map { case (k, v) => s"${render(k)} -> ${render(v)}" }.mkString("{", ", ", "}")
    case other => other.toString
  }

  /**
   * Describe the differing rows for a failure message.
   *
   * @param diffs        the differing rows to show (already truncated to maxErrors).
   * @param numUnequal   the total number of differing rows.
   * @param totalRows    the total number of rows compared.
   * @param fieldNames   column names, used to label fields.
   */
  def formatDiff(diffs: Seq[RowDiff], numUnequal: Long, totalRows: Long,
                 fieldNames: Seq[String], options: DataFrameEqualityOptions): String = {
    val percent = if (totalRows == 0) 0.0 else numUnequal * 100.0 / totalRows
    val header = f"$numUnequal of $totalRows rows differ ($percent%.5f %%)" +
      (if (diffs.size < numUnequal) s", showing the first ${diffs.size}" else "")

    def fieldName(idx: Int) = if (idx < fieldNames.size) fieldNames(idx) else s"_$idx"

    val body = diffs.map { case RowDiff(index, expected, actual) =>
      (expected, actual) match {
        case (Some(e), Some(a)) =>
          val width = math.max(e.length, a.length)
          val fields = (0 until width).map { idx =>
            val ev = if (idx < e.length) Some(e.get(idx)) else None
            val av = if (idx < a.length) Some(a.get(idx)) else None
            val same = (ev, av) match {
              case (Some(x), Some(y)) =>
                compareValues(x, y, options.tol, options.rtol, options.tolTimestamp)
              case _ => false
            }
            (idx, ev, av, same)
          }
          if (options.showOnlyDiff) {
            val lines = fields.filterNot(_._4).map { case (idx, ev, av, _) =>
              s"    ! ${fieldName(idx)}: expected=${ev.map(render).getOrElse("<missing>")}, " +
                s"actual=${av.map(render).getOrElse("<missing>")}"
            }
            (s"Row $index:" +: lines).mkString("\n")
          } else {
            def line(values: Row) = (0 until values.length).map { idx =>
              val marker = if (fields(idx)._4) "" else "!"
              marker + render(values.get(idx))
            }.mkString("[", ",", "]")
            s"Row $index:\n    expected: ${line(e)}\n    actual:   ${line(a)}"
          }
        case (Some(e), None) => s"Row $index: missing from actual: ${render(e)}"
        case (None, Some(a)) => s"Row $index: unexpected in actual: ${render(a)}"
        case (None, None) => s"Row $index: <none>"
      }
    }
    (header +: body).mkString("\n")
  }
}

/** Schema normalization and comparison used by the DataFrame assertions. */
object SchemaComparison {

  private val autoGeneratedAlias = "__autoGeneratedAlias"

  private def cleanMetadata(metadata: Metadata): Metadata = {
    if (!metadata.contains(autoGeneratedAlias)) {
      metadata
    } else {
      val metadataMap =
        parse(metadata.json).values.asInstanceOf[Map[String, Any]] - autoGeneratedAlias
      implicit val formats = org.json4s.DefaultFormats
      Metadata.fromJson(Serialization.write(metadataMap))
    }
  }

  /** Recursively strip internal metadata and (optionally) nullability. */
  def normalizeType(dataType: DataType, ignoreNullable: Boolean): DataType = dataType match {
    case StructType(fields) => StructType(fields.map(normalizeField(_, ignoreNullable)))
    case ArrayType(elementType, containsNull) =>
      ArrayType(normalizeType(elementType, ignoreNullable), ignoreNullable || containsNull)
    case MapType(keyType, valueType, valueContainsNull) =>
      MapType(normalizeType(keyType, ignoreNullable), normalizeType(valueType, ignoreNullable),
        ignoreNullable || valueContainsNull)
    case other => other
  }

  def normalizeField(field: StructField, ignoreNullable: Boolean): StructField =
    StructField(field.name, normalizeType(field.dataType, ignoreNullable),
      ignoreNullable || field.nullable, cleanMetadata(field.metadata))

  /**
   * Normalize the top level fields of a schema according to the options.
   * ignoreColumnName replaces names with positions, ignoreColumnOrder sorts by name.
   */
  def normalize(schema: StructType, ignoreNullable: Boolean, ignoreColumnOrder: Boolean,
                ignoreColumnName: Boolean): List[StructField] = {
    val fields = schema.fields.toList.map(normalizeField(_, ignoreNullable))
    val ordered = if (ignoreColumnOrder && !ignoreColumnName) fields.sortBy(_.name) else fields
    if (ignoreColumnName) {
      ordered.zipWithIndex.map { case (f, idx) => f.copy(name = idx.toString) }
    } else {
      ordered
    }
  }

  /** A readable, line per field description of the differences, for failure messages. */
  def describeDifferences(expected: List[StructField], actual: List[StructField]): String = {
    val width = math.max(expected.size, actual.size)
    val lines = (0 until width).map { idx =>
      val e = expected.lift(idx)
      val a = actual.lift(idx)
      val marker = if (e == a) "  " else "! "
      s"$marker$idx: expected=${e.map(_.toString).getOrElse("<missing>")}, " +
        s"actual=${a.map(_.toString).getOrElse("<missing>")}"
    }
    ("Schemas differ:" +: lines).mkString("\n")
  }

  /**
   * Whether Spark can sort on a column of this type. Maps, variants and
   * calendar intervals can not be sorted.
   */
  def isOrderable(dataType: DataType): Boolean = dataType match {
    case _: MapType => false
    case StructType(fields) => fields.forall(f => isOrderable(f.dataType))
    case ArrayType(elementType, _) => isOrderable(elementType)
    case udt: UserDefinedType[_] => isOrderable(udt.sqlType)
    case other => !Set("variant", "interval", "calendarinterval").contains(other.typeName)
  }

  /** Quote a column name so names with dots, spaces or backticks resolve correctly. */
  def quote(name: String): String = "`" + name.replace("`", "``") + "`"
}
