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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.connect.service.SparkConnectService
import org.scalatest.{BeforeAndAfterAll, Suite}

import com.holdenkarau.spark.testing.connect.ConnectBridge

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
  @transient private var _previousSession: SparkSession = _
  private lazy val _connectPort: Int = findFreePort()
  private var _isConnectSession: Boolean = false

  /**
   * Whether the primary `spark` session is routed through Connect.
   * True when SparkSession.Builder exposes .remote() (Spark 4.0+ unified API).
   */
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
        _previousSession = SparkSessionProvider._sparkSession
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
      // Only restore SparkSessionProvider if we actually replaced it in
      // beforeAll (the Some branch). On 3.5 the provider was never touched
      // and must not be clobbered with null.
      if (_connectSession != null) {
        _connectSession.close()
        _connectSession = null
        SparkSessionProvider._sparkSession = _previousSession
        _previousSession = null
      }
      _isConnectSession = false
      SparkConnectService.stop()
    } finally {
      super.afterAll()
    }
  }

  // Override the only RDD-based comparison with a collect-based version.
  // Everything else in the DataFrame assertions is pure DataFrame ops, so
  // Connect gets the same semantics and failure messages as classic Spark.
  override protected def compareRowsInOrder(
      expected: DataFrame, result: DataFrame,
      options: DataFrameEqualityOptions,
      maxRows: Int): RowComparison.OrderedComparison = {
    try {
      expected.cache()
      result.cache()
      val expectedRows = expected.collect()
      val resultRows = result.collect()
      if (expectedRows.length != resultRows.length) {
        RowComparison.OrderedComparison(
          expectedRows.length.toLong, resultRows.length.toLong, 0, Seq.empty)
      } else {
        val unequal = expectedRows.iterator.zip(resultRows.iterator).zipWithIndex
          .collect { case ((r1, r2), idx)
            if !RowComparison.compareRows(r1, r2, options) => (idx.toLong, r1, r2)
          }.toVector
        RowComparison.OrderedComparison(expectedRows.length.toLong,
          resultRows.length.toLong, unequal.size.toLong, unequal.take(maxRows))
      }
    } finally {
      expected.unpersist()
      result.unpersist()
    }
  }

}
