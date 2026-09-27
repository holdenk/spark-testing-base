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

package com.holdenkarau.spark.testing.connect

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.scalatest.funsuite.AnyFunSuite

import com.holdenkarau.spark.testing.{DataFrameAssertionsLike, TestSuite}

/**
 * Base for tests that run entirely over Spark Connect.
 *
 * Unlike `ConnectEnabled` in the main artifact, this does not start a Spark
 * session in the test JVM at all -- there is no SparkContext, no spark-sql,
 * nothing but the Connect client. That is what makes it work on Spark 3.5,
 * where spark-sql and spark-connect-client-jvm both define their own concrete
 * `org.apache.spark.sql.SparkSession` and cannot share a classloader.
 *
 * {{{
 * class MyTest extends ScalaConnectSuiteBase {
 *   test("runs over Connect") {
 *     val df = spark.sql("SELECT 1 AS value")
 *     assertDataFrameEquals(df, df)
 *   }
 * }
 * }}}
 *
 * Point it at a running server by overriding `connectRemote`, or with the
 * `spark.testing.connect.remote` system property or the `SPARK_REMOTE`
 * environment variable. Otherwise the suite tries to launch one itself (see
 * [[ConnectServerHarness]]), which needs a server classpath that currently only
 * this project's own build provides.
 *
 * The assertions come from `DataFrameAssertionsLike`, the same source the
 * classic `DataFrameSuiteBase` uses -- it is compiled twice, once against
 * spark-sql and once against the Connect client.
 */
trait ConnectSuiteBase extends BeforeAndAfterAll
    with TestSuite with DataFrameAssertionsLike { self: Suite =>

  @transient private var _spark: SparkSession = _
  @transient private var _harness: Option[ConnectServerHarness] = None

  // A lazy val rather than a def: `import spark.implicits._` needs a stable
  // identifier. It is forced on first use inside a test, by which time
  // beforeAll has filled _spark in.
  @transient override lazy val spark: SparkSession = {
    require(_spark != null,
      "Connect session not started yet (beforeAll has not run)")
    _spark
  }

  /**
   * The `sc://` URL of an already-running Connect server to use instead of
   * launching one. Override to point the suite at a server of your own; same
   * meaning as `ConnectEnabled.connectRemote` in the main artifact.
   */
  protected def connectRemote: Option[String] =
    ConnectServerHarness.configuredRemote

  /** The `sc://` URL this suite is actually talking to, once started. */
  protected def connectUrl: String = _url

  @transient private var _url: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    _url = connectRemote.getOrElse {
      val harness = ConnectServerHarness.start()
      _harness = Some(harness)
      harness.remote
    }
    try {
      _spark = SparkSession.builder().remote(_url).getOrCreate()
    } catch {
      // ScalaTest still runs afterAll when beforeAll throws an Exception, but
      // not for other Throwables (a LinkageError from a broken client
      // classpath, say), so stop the server here rather than rely on it.
      case t: Throwable =>
        try {
          _harness.foreach(_.stop())
        } finally {
          _harness = None
        }
        throw t
    }
  }

  override def afterAll(): Unit = {
    try {
      try {
        if (_spark != null) {
          // Close the field, not the lazy val: touching `spark` here would
          // force it in suites where no test ever did.
          _spark.close()
        }
      } finally {
        _spark = null
        // In its own finally: if closing the session throws, the child JVM
        // would otherwise outlive the suite.
        try {
          _harness.foreach(_.stop())
        } finally {
          _harness = None
        }
      }
    } finally {
      super.afterAll()
    }
  }
}

/** [[ConnectSuiteBase]] with ScalaTest's AnyFunSuite mixed in. */
trait ScalaConnectSuiteBase extends AnyFunSuite with ConnectSuiteBase
