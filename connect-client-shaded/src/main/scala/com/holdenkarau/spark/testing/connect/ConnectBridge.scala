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

// These imports compile against spark-connect-client-jvm's SparkSession.
// After sbt-assembly shading, bytecode references are rewritten to
// com.holdenkarau.spark.testing.connect.shaded.* so there is no
// classpath conflict with spark-sql's SparkSession in the core project.
import org.apache.spark.sql.SparkSession

/**
 * Bridge to create and use a Spark Connect client session from a shaded JAR.
 *
 * This class is compiled in a sub-project that only depends on
 * spark-connect-client-jvm (no spark-sql). The sbt-assembly shade rules
 * relocate org.apache.spark.sql.** so the resulting JAR can coexist
 * with spark-sql on the same classpath.
 *
 * All public methods use classloader-safe types (primitives, String,
 * Array[Any]) to avoid type mismatches between shaded and classic classes.
 */
object ConnectBridge {
  @volatile private var session: SparkSession = _

  def start(port: Int): SparkSession = synchronized {
    // Close any prior session so repeated start() calls don't leak.
    if (session != null) {
      session.close()
      session = null
    }
    session = SparkSession.builder()
      .remote(s"sc://localhost:$port")
      .getOrCreate()
  }

  def stop(): Unit = synchronized {
    if (session != null) {
      session.close()
      session = null
    }
  }

  def isActive: Boolean = session != null
}
