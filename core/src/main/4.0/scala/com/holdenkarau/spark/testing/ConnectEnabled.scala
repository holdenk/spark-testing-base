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

import java.time.Duration

import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.connect.EvilConnectService
import org.apache.spark.sql.connect.service.SparkConnectService
import org.scalatest.Suite

/**
 * :: Experimental ::
 * Mixin that routes an existing DataFrameSuiteBase test through Spark Connect.
 *
 * Add `with ConnectEnabled` to any suite extending DataFrameSuiteBase (or
 * ScalaDataFrameSuiteBase, or DatasetSuiteBase) and the `spark` session your
 * tests use becomes a Connect client session, so every DataFrame and SQL
 * operation goes over the Connect protocol:
 *
 * {{{
 * class MyTest extends ScalaDataFrameSuiteBase with ConnectEnabled {
 *   test("works through Connect") {
 *     val df = spark.read.parquet(...)
 *     assertDataFrameEquals(df, expected) // goes through Connect!
 *   }
 * }
 * }}}
 *
 * By default a Connect gRPC server is started inside the test JVM, on top of
 * the local SparkContext the suite already creates. To run against a server
 * that is already up instead, set the `spark.testing.connect.remote` system
 * property or the `SPARK_REMOTE` environment variable to its `sc://` URL --
 * note that the suite still creates a local SparkContext in that case, because
 * DataFrameSuiteBase is built on one.
 *
 * '''This requires Spark 4.0 or newer.''' On 4.0+ `org.apache.spark.sql.SparkSession`
 * is an abstract class in spark-sql-api that both the classic and the Connect
 * session extend, so a Connect session can stand in for a classic one. On Spark
 * 3.5 spark-sql and spark-connect-client-jvm each define their own concrete
 * class under that name and cannot share a classloader; testing 3.5 Connect
 * needs a client-only classpath instead.
 *
 * Anything that reaches past the DataFrame API will not work over Connect:
 * `.rdd`, `sc`, `sqlContext`, the DStream suite bases, the ScalaCheck
 * generators, and the codegen-mode test helpers.
 */
trait ConnectEnabled extends DatasetSuiteBase { self: Suite =>

  @transient private var _connectSession: SparkSession = _
  @transient private var _previousSession: SparkSession = _
  private var _startedServer: Boolean = false

  /** Whether the primary `spark` session is currently routed through Connect. */
  def isConnectSession: Boolean = _connectSession != null

  /**
   * The `sc://` URL of an already-running Connect server to use instead of
   * starting one in this JVM. Override to point tests at a real cluster.
   */
  protected def connectRemote: Option[String] = ConnectEnabled.externalRemote

  /**
   * Bind the in-JVM Connect server on an ephemeral port. We ask for port 0 and
   * read the port Spark actually bound back out of SparkConnectService, rather
   * than picking a free port ourselves and racing whoever grabs it next.
   */
  abstract override def conf: SparkConf =
    super.conf.set("spark.connect.grpc.binding.port", "0")

  override def beforeAll(): Unit = {
    super.beforeAll()

    val remote = connectRemote.getOrElse {
      SparkConnectService.start(sc)
      _startedServer = true
      s"sc://localhost:${EvilConnectService.localPort}"
    }

    // `spark` is a lazy val reading SparkSessionProvider._sparkSession, so the
    // swap has to happen here: after super.beforeAll() has built the classic
    // session, and before any test body forces `spark`.
    _previousSession = SparkSessionProvider._sparkSession
    // .connect() is load bearing. `remote(url)` only records the URL; which
    // companion the builder uses is still DEFAULT, which resolves to classic
    // whenever a classic session is already active -- and one always is here,
    // because super.beforeAll() just made it. You get a classic session back
    // and a "spark.connect.remote configuration is not supported in Classic
    // mode" warning buried in the logs. .connect() pins the companion.
    //
    // create() rather than getOrCreate() because the Connect companion caches
    // sessions by connection string and we bind an ephemeral port, so a cached
    // session from an earlier suite would point at a dead server.
    _connectSession = SparkSession.builder().connect().remote(remote).create()
    SparkSessionProvider._sparkSession = _connectSession
  }

  override def afterAll(): Unit = {
    try {
      // Deliberately NOT closing the Connect session here in the common case.
      // DataFrameSuiteBase's
      // afterAll already calls spark.stop() on it, and Spark's Connect client is
      // not idempotent about that: a second close re-sends ReleaseSession over
      // the channel the first one shut down, and the retry policy then sits
      // there for ~10 minutes before giving up. One close, done by the base
      // class, while the server is still listening.
      super.afterAll()
    } finally {
      try {
        // ...except when the suite reuses its context. Then
        // DataFrameSuiteBase.afterAll skips spark.stop() altogether and this is
        // the only close there is; without it the client's gRPC channel and its
        // retry threads leak for the life of the test JVM.
        if (reuseContextIfPossible && _connectSession != null) {
          _connectSession.close()
        }
      } finally {
        // In its own finally: if that close throws we would otherwise leave the
        // server running and, worse, leave a dead Connect session in the global
        // SparkSessionProvider for whatever suite runs next.
        if (_startedServer) {
          _startedServer = false
          SparkConnectService.stop()
        }
        // Only put the classic session back if we are still the ones in the
        // slot; DataFrameSuiteBase.afterAll nulls it out itself unless the
        // suite reuses its context.
        if (SparkSessionProvider._sparkSession eq _connectSession) {
          SparkSessionProvider._sparkSession = _previousSession
        }
        _connectSession = null
        _previousSession = null
      }
    }
  }

  /**
   * Connect sessions have no `sessionState`, so route configuration through
   * RuntimeConfig instead. Note this configures the *server's* session, and
   * that static SQL configs are rejected outright over Connect rather than
   * quietly ignored.
   */
  override protected def withSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
    val runtimeConf = spark.conf
    val (keys, values) = pairs.unzip
    val currentValues = keys.map(runtimeConf.getOption)
    keys.zip(values).foreach { case (k, v) => runtimeConf.set(k, v) }
    try f finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => runtimeConf.set(key, value)
        case (key, None) => runtimeConf.unset(key)
      }
    }
  }

  /**
   * There is no SQLContext over Connect, and the inherited one is worse than
   * missing: SparkSessionProvider.sqlContext ignores the session it is handed
   * and returns `SparkSession.builder.getOrCreate().sqlContext`, i.e. the
   * classic driver-side session. Left alone it would quietly route
   * `import sqlContext.implicits._` around Connect entirely, and the suite
   * would pass while testing nothing. Fail loudly instead.
   *
   * `impSqlContext` delegates here, so it is covered too.
   */
  @transient override lazy val sqlContext: SQLContext =
    throw new UnsupportedOperationException(
      "sqlContext is not available when running through Spark Connect. The " +
      "inherited one resolves to the classic driver-side session, which would " +
      "silently bypass Connect. Use `spark` instead -- " +
      "`import spark.implicits._` works the same way.")

  /**
   * Connect has no RDDs, so use the shared collect-based comparison rather
   * than the RDD join DataFrameSuiteBaseLike layers on top of it.
   * assertDataFrameEquals delegates here, and assertDataFrameDataEquals is
   * already pure DataFrame operations in the shared trait.
   */
  override def assertDataFrameApproximateEquals(
      expected: DataFrame, result: DataFrame,
      tol: Double, tolTimestamp: Duration,
      customShow: DataFrame => Unit = _.show()): Unit =
    assertDataFrameApproximateEqualsCollected(
      expected, result, tol, tolTimestamp, customShow)

  /**
   * Connect-safe Dataset comparison. The inherited version joins
   * `expected.rdd` with `result.rdd`, which does not exist over Connect.
   */
  override def assertDatasetEquals[U](expected: Dataset[U], result: Dataset[U])
      (implicit UCT: ClassTag[U]): Unit = {
    try {
      expected.cache()
      result.cache()

      val expectedRows = expected.collect()
      val resultRows = result.collect()

      assert("Length not Equal", expectedRows.length.toLong, resultRows.length.toLong)

      val unequal = expectedRows.zip(resultRows).zipWithIndex.filter {
        case ((o1, o2), _) => !o1.equals(o2)
      }
      assertEmpty(unequal.take(maxUnequalRowsToShow))
    } finally {
      expected.unpersist()
      result.unpersist()
    }
  }
}

object ConnectEnabled {
  /** System property naming an already-running Connect server. */
  val RemoteProperty = "spark.testing.connect.remote"

  /** Environment variable naming an already-running Connect server. */
  val RemoteEnvVar = "SPARK_REMOTE"

  /** The externally configured Connect server URL, if any. */
  def externalRemote: Option[String] =
    sys.props.get(RemoteProperty)
      .orElse(sys.env.get(RemoteEnvVar))
      .map(_.trim)
      .filter(_.nonEmpty)
}
