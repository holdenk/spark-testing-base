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
import java.nio.file.Files
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

/**
 * A Spark Connect server running in a child JVM.
 *
 * The Connect client cannot share a classloader with spark-sql on Spark 3.5,
 * so the server gets its own process. The build passes that process's
 * classpath in `spark.testing.connect.serverClasspath`; see the `connect`
 * project in build.sbt.
 *
 * @param process the server JVM
 * @param remote  its `sc://` URL
 */
class ConnectServerHarness(process: Process, val remote: String) {
  def stop(): Unit = {
    process.destroy()
    if (!process.waitFor(30, TimeUnit.SECONDS)) {
      process.destroyForcibly()
    }
  }
}

object ConnectServerHarness {

  /** System property naming an already-running Connect server. */
  val RemoteProperty = "spark.testing.connect.remote"

  /** Environment variable naming an already-running Connect server. */
  val RemoteEnvVar = "SPARK_REMOTE"

  /** System property holding the classpath to launch the server JVM with. */
  val ServerClasspathProperty = "spark.testing.connect.serverClasspath"

  /** How long to wait for the child JVM to report its port. */
  private val StartupTimeoutMillis = 300000L

  /** An externally managed Connect server, if the user pointed us at one. */
  def configuredRemote: Option[String] =
    sys.props.get(RemoteProperty)
      .orElse(sys.env.get(RemoteEnvVar))
      .map(_.trim)
      .filter(_.nonEmpty)

  /**
   * Launch a Connect server in a child JVM and wait for it to report the
   * ephemeral port it bound.
   */
  def start(): ConnectServerHarness = {
    val classpath = sys.props.getOrElse(ServerClasspathProperty,
      throw new IllegalStateException(
        s"$ServerClasspathProperty is not set, so there is no way to launch a " +
        "Spark Connect server. Either run these tests through sbt, which sets " +
        s"it, or point them at a running server with -D$RemoteProperty=sc://host:port."))

    val portFile = File.createTempFile("spark-connect-port", ".txt")
    // The server writes the file itself; it must not exist when it starts.
    portFile.delete()
    portFile.deleteOnExit()

    val java = new File(new File(System.getProperty("java.home"), "bin"), "java")
    val command = (Seq(java.getAbsolutePath) ++
      addOpens ++
      Seq("-cp", classpath,
        "com.holdenkarau.spark.testing.connect.ConnectServerMain",
        portFile.getAbsolutePath)).asJava

    val builder = new ProcessBuilder(command)
    builder.redirectErrorStream(true)
    builder.redirectOutput(ProcessBuilder.Redirect.INHERIT)
    val process = builder.start()

    val port = awaitPort(process, portFile)
    new ConnectServerHarness(process, s"sc://localhost:$port")
  }

  private def awaitPort(process: Process, portFile: File): Int = {
    val deadline = System.currentTimeMillis() + StartupTimeoutMillis
    while (System.currentTimeMillis() < deadline) {
      if (!process.isAlive) {
        throw new IllegalStateException(
          s"Spark Connect server exited with ${process.exitValue()} before " +
          "reporting a port; its output is above.")
      }
      if (portFile.exists()) {
        val text =
          new String(Files.readAllBytes(portFile.toPath), StandardCharsets.UTF_8).trim
        if (text.nonEmpty) {
          return text.toInt
        }
      }
      Thread.sleep(200)
    }
    process.destroyForcibly()
    throw new IllegalStateException(
      s"Spark Connect server did not report a port within " +
      s"${StartupTimeoutMillis / 1000} seconds.")
  }

  /**
   * The same --add-opens flags the sbt build passes to its own forked test
   * JVMs; Spark needs them on JDK 17+.
   */
  private def addOpens: Seq[String] = {
    if (System.getProperty("java.specification.version") > "1.17") {
      Seq(
        "base/java.lang", "base/java.lang.invoke", "base/java.lang.reflect",
        "base/java.io", "base/java.net", "base/java.nio",
        "base/java.util", "base/java.util.concurrent",
        "base/java.util.concurrent.atomic",
        "base/sun.nio.ch", "base/sun.nio.cs", "base/sun.security.action",
        "base/sun.util.calendar", "security.jgss/sun.security.krb5"
      ).map("--add-opens=java." + _ + "=ALL-UNNAMED")
    } else {
      Seq.empty
    }
  }
}
