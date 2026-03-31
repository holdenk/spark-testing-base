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

import java.net.ServerSocket
import java.time.Duration

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.connect.service.SparkConnectService
import com.holdenkarau.spark.testing.connect.ConnectBridge
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
 * :: Experimental ::
 * Mixin that routes an existing DataFrameSuiteBase test through Spark Connect.
 *
 * Just add `with ConnectEnabled` to any test that extends DataFrameSuiteBase
 * (or ScalaDataFrameSuiteBase) and all DataFrame/SQL operations will go through
 * the Connect protocol.
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
 * On Spark 4.0+, the unified SparkSession API supports .remote() and the
 * `spark` session is fully routed through Connect. On Spark 3.5.x, the
 * Connect gRPC server is started and a shaded Connect client (from the
 * connect-client-shaded sub-project) validates connectivity. Use
 * `isConnectSession` to check if the primary session is Connect-based.
 */
trait ConnectEnabled extends BeforeAndAfterAll with DataFrameSuiteBaseLike {
  self: Suite with SparkContextProvider =>

  @transient private var _connectSession: SparkSession = _
  private lazy val _connectPort: Int = findFreePort()
  private var _isConnectSession: Boolean = false

  /** Whether the primary `spark` session goes through Connect (true on 4.0+). */
  def isConnectSession: Boolean = _isConnectSession

  /**
   * Whether the Connect gRPC server is running and reachable via the
   * shaded bridge (true on both 3.5+ and 4.0+).
   */
  def isConnectServerActive: Boolean = ConnectBridge.isActive

  private def findFreePort(): Int = {
    val socket = new ServerSocket(0)
    try {
      socket.getLocalPort
    } finally {
      socket.close()
    }
  }

  /** Inject the Connect gRPC port into the server SparkConf. */
  abstract override def conf: SparkConf = {
    super.conf.set("spark.connect.grpc.binding.port", _connectPort.toString)
  }

  /**
   * Try to create a Connect client SparkSession using reflection.
   * Returns Some(session) on Spark 4.0+ (unified API with .remote()),
   * None on Spark 3.5 (classic Builder without .remote()).
   */
  private def tryCreateConnectSession(port: Int): Option[SparkSession] = {
    val builder = SparkSession.builder()
    try {
      val remoteMethod = builder.getClass.getMethod("remote", classOf[String])
      val connectedBuilder = remoteMethod.invoke(builder, s"sc://localhost:$port")
      val session = connectedBuilder.getClass
        .getMethod("getOrCreate")
        .invoke(connectedBuilder)
        .asInstanceOf[SparkSession]
      Some(session)
    } catch {
      case _: NoSuchMethodException => None
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    SparkConnectService.start(SparkContext.getOrCreate())

    // On 4.0+, replace the primary session with a Connect session.
    // On 3.5, the classic session stays but the shaded bridge provides
    // Connect validation.
    tryCreateConnectSession(_connectPort) match {
      case Some(session) =>
        _connectSession = session
        _isConnectSession = true
        SparkSessionProvider._sparkSession = _connectSession
      case None =>
    }

    // Start the shaded bridge (works on both 3.5 and 4.0+)
    ConnectBridge.start(_connectPort)
  }

  override def afterAll(): Unit = {
    try {
      ConnectBridge.stop()
      if (_connectSession != null) {
        _connectSession.close()
        _connectSession = null
      }
      SparkConnectService.stop()
    } finally {
      super.afterAll()
    }
  }

  // Override the only RDD-based assertion with a collect-based version.
  // assertDataFrameDataEquals is already pure DF ops in the base class.
  override def assertDataFrameApproximateEquals(
      expected: DataFrame, result: DataFrame,
      tol: Double, tolTimestamp: Duration,
      customShow: DataFrame => Unit = _.show()): Unit = {

    assertSchemasEqual(expected.schema, result.schema)

    val expectedRows = expected.collect()
    val resultRows = result.collect()

    assert("Length not Equal", expectedRows.length.toLong, resultRows.length.toLong)

    val unequalRows = expectedRows.zip(resultRows).zipWithIndex.filter {
      case ((r1, r2), _) =>
        !(r1.equals(r2) ||
          DataFrameSuiteBase.approxEquals(r1, r2, tol, tolTimestamp))
    }

    if (unequalRows.nonEmpty) {
      val sample = unequalRows.take(maxUnequalRowsToShow)
      val message = sample.map { case ((r1, r2), idx) =>
        s"Row $idx: expected=$r1, actual=$r2"
      }.mkString("\n")
      fail(s"There are some unequal rows:\n$message")
    }
  }

}
