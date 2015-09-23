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

import org.apache.spark._

import java.io.{File, FileOutputStream, OutputStreamWriter}
import java.util.Properties
import java.util.concurrent.TimeUnit

import scala.collection.JavaConversions._
import scala.collection.mutable

import com.google.common.base.Charsets.UTF_8
import com.google.common.io.ByteStreams
import com.google.common.io.Files
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.MiniYARNCluster
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}


import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite

/** Shares an HDFS MiniCluster based `SparkContext` between all tests in a suite and
 * closes it at the end */
trait SharedMiniCluster extends BeforeAndAfterAll { self: Suite =>

  // log4j configuration for the YARN containers, so that their output is collected
  // by YARN instead of trying to overwrite unit-tests.log.
  private val LOG4J_CONF = """
    |log4j.rootCategory=DEBUG, console
    |log4j.appender.console=org.apache.log4j.ConsoleAppender
    |log4j.appender.console.target=System.err
    |log4j.appender.console.layout=org.apache.log4j.PatternLayout
    |log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n
    """.stripMargin

  @transient private var _sc: SparkContext = _
  @transient private var yarnCluster: MiniYARNCluster = null
  private var tempDir: File = _
  private var fakeSparkJar: File = _
  private var hadoopConfDir: File = _
  private var logConfDir: File = _

  def sc: SparkContext = _sc

  override def beforeAll() {
    tempDir = Utils.createTempDir();
    logConfDir = new File(tempDir, "log4j")
    logConfDir.mkdir()
    System.setProperty("SPARK_YARN_MODE", "true")

    val master = "yarn-client"
    val conf = new SparkConf().setMaster(master).setAppName("test")
    val logConfFile = new File(logConfDir, "log4j.properties")
    Files.write(LOG4J_CONF, logConfFile, UTF_8)

    yarnCluster = new MiniYARNCluster(getClass().getName(), 1, 1, 1)
    yarnCluster.init(new YarnConfiguration())
    yarnCluster.start()
    val config = yarnCluster.getConfig()
    val deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10)
    while (config.get(YarnConfiguration.RM_ADDRESS).split(":")(1) == "0") {
      if (System.currentTimeMillis() > deadline) {
        throw new IllegalStateException("Timed out waiting for RM to come up.")
      }
      TimeUnit.MILLISECONDS.sleep(100)
    }

    fakeSparkJar = File.createTempFile("sparkJar", null, tempDir)
    sys.props += ("spark.yarn.jar" -> ("local:" + fakeSparkJar.getAbsolutePath()))
    sys.props += ("spark.executor.instances" -> "1")
    val childClasspath = logConfDir.getAbsolutePath() + File.pathSeparator +
      sys.props("java.class.path")
    sys.props += ("spark.driver.extraClassPath" -> childClasspath)
    sys.props += ("spark.executor.extraClassPath" -> childClasspath)

    _sc = new SparkContext(conf)
    super.beforeAll()
  }

  override def afterAll() {
    yarnCluster.stop()
    _sc.stop()
    System.clearProperty("SPARK_YARN_MODE")
    _sc = null
    super.afterAll()
  }
}
