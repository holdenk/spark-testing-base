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

import org.scalatest.{BeforeAndAfterAll, Suite}

/**
 * Shares an HDFS MiniCluster based `SparkContext` between all tests in a suite and
 * closes it at the end. This requires that the env variable SPARK_HOME is set.
 * Further more if this is used, all Spark tests must run against the yarn mini cluster
 * (see https://issues.apache.org/jira/browse/SPARK-10812 for details).
 */
trait SharedMiniCluster extends BeforeAndAfterAll with HDFSClusterLike with YARNClusterLike { self: Suite =>

  override def beforeAll() {
    // Try and do setup, and in-case we fail shutdown
    try {
      super.startHDFS()
      super.startYARN()
    } catch {
      case e: Exception =>
		super.shutdownYARN()
		super.shutdownHDFS()
        throw e
    }
    super.beforeAll()
  }

  override def afterAll() {
    super.shutdownYARN()
    super.shutdownHDFS()
    super.afterAll()
  }
}
