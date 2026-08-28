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
import java.time.Duration

import scala.math.abs

import org.apache.spark.sql.Row

/**
 * Row-level approximate comparison.
 *
 * This lives apart from DataFrameSuiteBase because it depends on nothing but
 * `Row` and the standard library, which means it can be compiled against the
 * Spark Connect client as well as against classic spark-sql.
 * `DataFrameSuiteBase.approxEquals` delegates here.
 */
object RowComparisons {

  /** Approximate equality, based on equals from [[Row]] */
  def approxEquals(r1: Row, r2: Row, tol: Double): Boolean =
    approxEquals(r1, r2, tol, Duration.ofNanos((tol*1000).toLong))

  /** Approximate equality, based on equals from [[Row]] */
  def approxEquals(r1: Row, r2: Row, tolTimestamp: Duration): Boolean =
    approxEquals(r1, r2, 0, tolTimestamp)

  private def compareTimestamp(t1: Timestamp, t2: Timestamp,
                               tolTimestamp: Duration): Boolean = {
    !(Duration.between(t1.toInstant, t2.toInstant).abs.compareTo(tolTimestamp) > 0)
  }

  private def compareDouble(d1: Double, d2: Double, tol: Double): Boolean =
    !((java.lang.Double.isNaN(d1) != java.lang.Double.isNaN(d2)) || (abs(d1 - d2) > tol))


  private def compareFloat(f1: Float, f2: Float, tol: Double): Boolean = {
    if (java.lang.Float.isNaN(f1) != java.lang.Float.isNaN(f2)) {
      return false
    }
    if (abs(f1 - f2) > tol) {
      return false
    }
    true
  }

  private def compareJavaBigDecimal(d1: java.math.BigDecimal,
                                    d2: java.math.BigDecimal,
                                    tol: Double): Boolean = {
    if (d1.compareTo(d2) != 0) {
      if (d1.subtract(d2).abs.compareTo(new java.math.BigDecimal(tol)) > 0) {
        return false
      }
    }
    true
  }

  private def compareScalaBigDecimal(d1: scala.math.BigDecimal,
                                    d2: scala.math.BigDecimal,
                                    tol: Double): Boolean = {
    if ((d1 - d2).abs > tol) {
      return false
    }
    true
  }

  /** Approximate equality, based on equals from [[Row]] */
  def approxEquals(r1: Row, r2: Row, tol: Double,
                   tolTimestamp: Duration): Boolean = {
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
              if (!compareFloat(f1, o2.asInstanceOf[Float], tol)) {
                return false
              }

            case d1: Double =>
              if (!compareDouble(d1, o2.asInstanceOf[Double], tol)) {
                return false
              }

            case d1: java.math.BigDecimal =>
              if (!compareJavaBigDecimal(d1, o2.asInstanceOf[java.math.BigDecimal], tol)) {
                return false
              }

            case d1: scala.math.BigDecimal =>
              if (!compareScalaBigDecimal(d1, o2.asInstanceOf[scala.math.BigDecimal], tol)) {
                return false
              }

            case t1: Timestamp =>
              if (!compareTimestamp(t1, o2.asInstanceOf[Timestamp], tolTimestamp)) {
                return false
              }

            case row1: Row =>
              if (!approxEquals(row1, o2.asInstanceOf[Row], tol, tolTimestamp)) {
                return false
              }

            case head :: _ if head.isInstanceOf[Row] =>
              o1.asInstanceOf[Seq[Row]].zip(o2.asInstanceOf[Seq[Row]]).foreach {
                case (row1, row2) if !approxEquals(row1, row2, tol, tolTimestamp) =>
                  return false
                case _ =>
              }

            case head :: _ if head.isInstanceOf[Timestamp] =>
              o1.asInstanceOf[Seq[Timestamp]].zip(o2.asInstanceOf[Seq[Timestamp]]).foreach {
                case (t1, t2) if !compareTimestamp(t1, t2, tolTimestamp) =>
                  return false
                case _ =>
              }

            case head :: _ if head.isInstanceOf[Double] =>
              o1.asInstanceOf[Seq[Double]].zip(o2.asInstanceOf[Seq[Double]]).foreach {
                case (d1, d2) if !compareDouble(d1, d2, tol) =>
                  return false
                case _ =>
              }

            case head :: _ if head.isInstanceOf[Float] =>
              o1.asInstanceOf[Seq[Float]].zip(o2.asInstanceOf[Seq[Float]]).foreach {
                case (f1, f2) if !compareFloat(f1, f2, tol) =>
                  return false
                case _ =>
              }

            case head :: _ if head.isInstanceOf[java.math.BigDecimal] =>
              o1.asInstanceOf[Seq[java.math.BigDecimal]].zip(o2.asInstanceOf[Seq[java.math.BigDecimal]]).foreach {
                case (d1, d2) if !compareJavaBigDecimal(d1, d2, tol) =>
                  return false
                case _ =>
              }

            case head :: _ if head.isInstanceOf[scala.math.BigDecimal] =>
              o1.asInstanceOf[Seq[scala.math.BigDecimal]].zip(o2.asInstanceOf[Seq[scala.math.BigDecimal]]).foreach {
                case (d1, d2) if !compareScalaBigDecimal(d1, d2, tol) =>
                  return false
                case _ =>
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
