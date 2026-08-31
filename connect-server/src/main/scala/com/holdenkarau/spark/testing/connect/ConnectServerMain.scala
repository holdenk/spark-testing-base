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

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.EvilConnectService
import org.apache.spark.sql.connect.service.SparkConnectService

/**
 * A standalone Spark Connect server, for tests that cannot host one in-process.
 *
 * On Spark 3.5, spark-sql and spark-connect-client-jvm each define their own
 * concrete `org.apache.spark.sql.SparkSession`, so a client and a server can
 * never share a classloader. The Connect suites therefore run with only the
 * client on their classpath and start this in a separate JVM, whose classpath
 * has spark-sql and spark-connect but no client.
 *
 * Usage: `ConnectServerMain <port-file>`. The gRPC server binds an ephemeral
 * port; once it is up the port is written to `<port-file>`, which is how the
 * parent process learns where to connect. Writing to a temp file and renaming
 * keeps the parent from ever reading a half-written port.
 *
 * The process then parks until it is killed.
 */
object ConnectServerMain {

  def main(args: Array[String]): Unit = {
    require(args.length == 1,
      "usage: ConnectServerMain <port-file>")
    val portFile = new File(args(0))

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("spark-testing-base-connect-server")
      .set("spark.ui.enabled", "false")
      .set("spark.driver.host", "localhost")
      // Bind an ephemeral port; we report back the one we actually got.
      .set("spark.connect.grpc.binding.port", "0")

    val session = SparkSession.builder().config(conf).getOrCreate()
    SparkConnectService.start(session.sparkContext)

    val tmp = new File(portFile.getAbsolutePath + ".tmp")
    Files.write(tmp.toPath,
      EvilConnectService.localPort.toString.getBytes(StandardCharsets.UTF_8))
    Files.move(tmp.toPath, portFile.toPath,
      java.nio.file.StandardCopyOption.ATOMIC_MOVE)

    // Nothing else to do; the parent kills us when the suite is finished.
    Thread.currentThread().join()
  }
}
