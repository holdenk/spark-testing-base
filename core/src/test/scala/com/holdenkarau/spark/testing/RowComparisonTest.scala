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
import org.scalatest.funsuite.AnyFunSuite

/** Tests for RowComparison / SchemaComparison, these do not need Spark running. */
class RowComparisonTest extends AnyFunSuite {
  import RowComparison._

  private val NaN = Double.NaN
  private val Inf = Double.PositiveInfinity
  private val tolerances = Seq((0.0, 0.0), (1e-8, 1e-5), (10.0, 0.5))

  private def same(a: Any, b: Any, tol: Double = 0.0, rtol: Double = 0.0,
                 tolTimestamp: Duration = Duration.ZERO) =
    compareValues(a, b, tol, rtol, tolTimestamp)

  // Mirrors the cases from SPARK-59514 (apache/spark#58790).
  test("non-finite doubles never match finite values or the other infinity") {
    val unequal = Seq((NaN, 1.0), (NaN, Inf), (NaN, -Inf), (1.0, Inf), (1.0, -Inf), (Inf, -Inf))
    for ((tol, rtol) <- tolerances; (a, b) <- unequal; (x, y) <- Seq((a, b), (b, a))) {
      assert(!same(x, y, tol, rtol), s"$x vs $y with tol=$tol rtol=$rtol")
      assert(!same(x.toFloat, y.toFloat, tol, rtol), s"float $x vs $y")
    }
  }

  test("non-finite doubles match themselves") {
    for ((tol, rtol) <- tolerances; v <- Seq(NaN, Inf, -Inf)) {
      assert(same(v, v, tol, rtol))
      assert(same(v.toFloat, v.toFloat, tol, rtol))
    }
  }

  test("non-finite values nested in structs, arrays and maps") {
    val wrappers: Seq[Double => Any] = Seq(
      v => Row(Row(v)),
      v => Seq(v),
      v => Array(v),
      v => Map("key" -> v),
      v => Row(Seq(Row(Map("k" -> v)))))
    for (wrap <- wrappers) {
      assert(!same(wrap(NaN), wrap(1.0), 10.0))
      assert(!same(wrap(1.0), wrap(NaN), 10.0))
      assert(same(wrap(NaN), wrap(NaN)))
      assert(!same(wrap(Inf), wrap(-Inf), 10.0, 0.5))
    }
  }

  test("absolute and relative tolerance") {
    assert(same(1.0, 1.05, tol = 0.1))
    assert(!same(1.0, 1.2, tol = 0.1))
    assert(same(1000.0, 1001.0, rtol = 0.01))
    assert(!same(1000.0, 1011.0, rtol = 0.01))
    // relative to the expected (first) value
    assert(same(100.0, 109.0, rtol = 0.1))
    assert(!same(0.0, 1e-10))
    assert(same(0.0, -0.0))
  }

  test("floats are widened before subtracting") {
    assert(same(1.1f, 1.2f, tol = 0.2))
    assert(!same(1.1f, 1.2f, tol = 0.05))
  }

  test("decimals") {
    val one = new java.math.BigDecimal("1.00")
    assert(same(one, new java.math.BigDecimal("1.0")))
    assert(same(one, new java.math.BigDecimal("1.05"), tol = 0.1))
    assert(!same(one, new java.math.BigDecimal("1.5"), tol = 0.1))
    assert(same(one, new java.math.BigDecimal("1.05"), rtol = 0.1))
    assert(same(BigDecimal("1.0"), BigDecimal("1.05"), tol = 0.1))
  }

  test("temporal values") {
    val ts = Timestamp.valueOf("2018-01-12 20:22:13")
    val ts2 = Timestamp.valueOf("2018-01-12 20:22:15")
    assert(same(ts, ts2, tolTimestamp = Duration.ofSeconds(2)))
    assert(!same(ts, ts2, tolTimestamp = Duration.ofSeconds(1)))
    val instant = Instant.parse("2018-01-12T20:22:13Z")
    assert(same(instant, instant.plusSeconds(2), tolTimestamp = Duration.ofSeconds(2)))
    assert(!same(instant, instant.plusSeconds(3), tolTimestamp = Duration.ofSeconds(2)))
    val ldt = LocalDateTime.of(2018, 1, 12, 20, 22, 13)
    assert(same(ldt, ldt.plusSeconds(2), tolTimestamp = Duration.ofSeconds(2)))
    assert(!same(ldt, ldt.plusSeconds(3), tolTimestamp = Duration.ofSeconds(2)))
  }

  test("sequences must have the same length and handle nulls and empties") {
    assert(!same(Seq(1.0), Seq(1.0, 2.0), tol = 10))
    assert(!same(Seq(1.0, 2.0), Seq(1.0), tol = 10))
    assert(same(Seq.empty[Double], Seq.empty[Double]))
    assert(same(Seq[Any](null, 1.0), Seq[Any](null, 1.05), tol = 0.1))
    assert(!same(Seq[Any](null, 1.0), Seq[Any](1.0, 1.0), tol = 0.1))
    assert(same(Seq(Row(1.0), null), Seq(Row(1.05), null), tol = 0.1))
    // Vector, ArrayBuffer etc. are all Seqs
    assert(same(Vector(1.0), scala.collection.mutable.ArrayBuffer(1.05), tol = 0.1))
  }

  test("rows must have the same length") {
    // SPARK-59513: trailing fields must not be ignored
    assert(!compareRows(Row(1, 2), Row(1, 2, 3), 0.0, 0.0, Duration.ZERO))
    assert(!compareRows(Row(1, 2, 3), Row(1, 2), 0.0, 0.0, Duration.ZERO))
    assert(compareRows(null, null, 0.0, 0.0, Duration.ZERO))
    assert(!compareRows(Row(1), null, 0.0, 0.0, Duration.ZERO))
  }

  test("maps compare values with tolerance and keys exactly") {
    assert(same(Map("a" -> 1.0, "b" -> 2.0), Map("b" -> 2.05, "a" -> 1.0), tol = 0.1))
    assert(!same(Map("a" -> 1.0), Map("b" -> 1.0)))
    assert(!same(Map("a" -> 1.0), Map("a" -> 1.0, "b" -> 1.0)))
  }

  test("mismatched types return false rather than throwing") {
    assert(!same(1.0f, "1"))
    assert(!same("1", 1.0f))
    assert(!same(Seq(1.0), Row(1.0)))
    assert(!same(new java.math.BigDecimal(1), "1"))
  }

  test("binary") {
    assert(same(Array[Byte](1, 2), Array[Byte](1, 2)))
    assert(!same(Array[Byte](1, 2), Array[Byte](1, 3)))
  }

  test("formatDiff reports counts, marks fields and honours showOnlyDiff") {
    val diffs = Seq(RowDiff(3, Some(Row(1, "same")), Some(Row(2, "same"))))
    val message = formatDiff(diffs, numUnequal = 5, totalRows = 10,
      fieldNames = Seq("i", "s"), DataFrameEqualityOptions())
    assert(message.contains("5 of 10 rows differ (50.00000 %), showing the first 1"))
    assert(message.contains("expected: [!1,same]"))
    assert(message.contains("actual:   [!2,same]"))
    val onlyDiff = formatDiff(diffs, 1, 10, Seq("i", "s"),
      DataFrameEqualityOptions(showOnlyDiff = true))
    assert(onlyDiff.contains("! i: expected=1, actual=2"))
    assert(!onlyDiff.contains("same"))
  }

  test("schema normalization ignores nested nullability only when asked") {
    val nullable = StructType(Seq(
      StructField("a", ArrayType(DoubleType, containsNull = true), nullable = true),
      StructField("m", MapType(StringType,
        StructType(Seq(StructField("x", IntegerType, nullable = true))),
        valueContainsNull = true))))
    val notNullable = StructType(Seq(
      StructField("a", ArrayType(DoubleType, containsNull = false), nullable = false),
      StructField("m", MapType(StringType,
        StructType(Seq(StructField("x", IntegerType, nullable = false))),
        valueContainsNull = false))))
    assert(SchemaComparison.normalize(nullable, false, false, false) !=
      SchemaComparison.normalize(notNullable, false, false, false))
    assert(SchemaComparison.normalize(nullable, true, false, false) ===
      SchemaComparison.normalize(notNullable, true, false, false))
  }

  test("schema normalization for column order and names") {
    val ab = StructType(Seq(StructField("a", IntegerType), StructField("b", StringType)))
    val ba = StructType(Seq(StructField("b", StringType), StructField("a", IntegerType)))
    val xy = StructType(Seq(StructField("x", IntegerType), StructField("y", StringType)))
    assert(SchemaComparison.normalize(ab, false, true, false) ===
      SchemaComparison.normalize(ba, false, true, false))
    assert(SchemaComparison.normalize(ab, false, false, true) ===
      SchemaComparison.normalize(xy, false, false, true))
  }

  test("orderable types") {
    assert(SchemaComparison.isOrderable(DoubleType))
    assert(SchemaComparison.isOrderable(ArrayType(StructType(Seq(StructField("a", StringType))))))
    assert(!SchemaComparison.isOrderable(MapType(StringType, StringType)))
    assert(!SchemaComparison.isOrderable(
      StructType(Seq(StructField("m", MapType(StringType, StringType))))))
  }

  test("quote") {
    assert(SchemaComparison.quote("a.b") === "`a.b`")
    assert(SchemaComparison.quote("a`b") === "`a``b`")
  }
}
