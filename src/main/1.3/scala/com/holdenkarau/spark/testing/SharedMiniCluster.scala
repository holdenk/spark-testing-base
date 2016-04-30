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
import java.net.URLClassLoader

import scala.collection.JavaConversions._
import scala.collection.mutable

import com.google.common.base.Charsets.UTF_8
import com.google.common.io.ByteStreams
import com.google.common.io.Files
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.MiniYARNCluster
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}


import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite

/**
 * Shares an HDFS MiniCluster based `SparkContext` between all tests in a suite and
 * closes it at the end. This requires that the env variable SPARK_HOME is set.
 * Further more if this is used, all Spark tests must run against the yarn mini cluster
 * (see https://issues.apache.org/jira/browse/SPARK-10812 for details).
 */
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

  private val configurationFilePath = new File(
    this.getClass.getProtectionDomain().getCodeSource().getLocation().getPath())
    .getParentFile.getAbsolutePath + "/hadoop-site.xml"

  @transient private var _sc: SparkContext = _
  @transient private var yarnCluster: MiniYARNCluster = null
  var miniDFSCluster: MiniDFSCluster = null
  private var tempDir: File = _
  private var hadoopConfDir: File = _
  private var logConfDir: File = _

  def sc: SparkContext = _sc
  val master = "yarn-client"

  // Program specific class path, override if this isn't working for you
  // TODO: This is a hack, but classPathFromCurrentClassLoader isn't sufficient :(
  def extraClassPath(): Seq[String] = {
    List(
      // Likely sbt classes & test-classes directory
      new File("target/scala-2.10/classes"),
      new File("target/scala-2.10/test-classes"),
      // Likely maven classes & test-classes directory
      new File("target/classes"),
      new File("target/test-classes")
    ).map(_.getAbsolutePath).filter(_ != null)
  }

  // Class path based on current env + program specific class path.
  def classPathFromCurrentClassLoader(): Seq[String] = {
    // This _assumes_ that either the current class loader or parent class loader is a
    // URLClassLoader
    val urlClassLoader = Thread.currentThread().getContextClassLoader() match {
      case uc: URLClassLoader => uc
      case xy => xy.getParent.asInstanceOf[URLClassLoader]
    }
    urlClassLoader.getURLs().toSeq.map(u => new File(u.toURI()).getAbsolutePath())
  }

  def generateClassPath(): String = {
    // Class path
    val clList = (List(logConfDir.getAbsolutePath(), sys.props("java.class.path")) ++
      classPathFromCurrentClassLoader() ++ extraClassPath())
    val clPath = clList.mkString(File.pathSeparator)
    clPath
  }


  override def beforeAll() {
    // Try and do setup, and in-case we fail shutdown
    try {
      setup()
    } catch {
      case e: Exception =>
        shutdown()
        throw e
    }
    super.beforeAll()
  }

  def setup() {
    tempDir = Utils.createTempDir()
    logConfDir = new File(tempDir, "log4j")
    logConfDir.mkdir()
    System.setProperty("SPARK_YARN_MODE", "true")

    val logConfFile = new File(logConfDir, "log4j.properties")
    Files.write(LOG4J_CONF, logConfFile, UTF_8)

    val yarnConf = new YarnConfiguration()
    yarnCluster = new MiniYARNCluster(getClass().getName(), 1, 1, 1)
    yarnCluster.init(yarnConf)
    yarnCluster.start()
    val config = yarnCluster.getConfig()
    val deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10)
    while (config.get(YarnConfiguration.RM_ADDRESS).split(":")(1) == "0") {
      if (System.currentTimeMillis() > deadline) {
        throw new IllegalStateException("Timed out waiting for RM to come up.")
      }
      TimeUnit.MILLISECONDS.sleep(100)
    }

    // Find the spark assembly jar
    // TODO: Better error messaging
    val sparkAssemblyDir = sys.env("SPARK_HOME") + "/assembly/target/scala-2.10/"
    val sparkLibDir = sys.env("SPARK_HOME") + "/lib/"
    val candidates = List(new File(sparkAssemblyDir).listFiles,
      new File(sparkLibDir).listFiles).filter(_ != null).flatMap(_.toSeq)
    val sparkAssemblyJar = candidates.find{f =>
      val name = f.getName
      name.endsWith(".jar") && name.startsWith("spark-assembly")}
      .getOrElse(throw new Exception(
        "Failed to find spark assembly jar, make sure SPARK_HOME is set correctly"))
      .getAbsolutePath()

    println("Spark assembly Jar: " + sparkAssemblyJar)

    // Set some yarn props
    sys.props += ("spark.yarn.jar" -> ("local:" + sparkAssemblyJar))
    sys.props += ("spark.executor.instances" -> "1")
    // Figure out our class path
    val childClasspath = generateClassPath()
    sys.props += ("spark.driver.extraClassPath" -> childClasspath)
    sys.props += ("spark.executor.extraClassPath" -> childClasspath)
    val configurationFile = new File(configurationFilePath)
    if (configurationFile.exists()) {
      configurationFile.delete()
    }
    val configuration = yarnCluster.getConfig
    iterableAsScalaIterable(configuration).foreach { e =>
      sys.props += ("spark.hadoop." + e.getKey() -> e.getValue())
    }
    configuration.writeXml(new FileOutputStream(configurationFile))
    // Copy the system props
    val props = new Properties()
    sys.props.foreach { case (k, v) =>
      if (k.startsWith("spark.")) {
        props.setProperty(k, v)
      }
    }
    val propsFile = File.createTempFile("spark", ".properties", tempDir)
    val writer = new OutputStreamWriter(new FileOutputStream(propsFile), UTF_8)
    props.store(writer, "Spark properties.")
    writer.close()


    // Set up DFS
    val hdfsConf = new HdfsConfiguration(yarnConf)
    miniDFSCluster = new MiniDFSCluster.Builder(hdfsConf)
      .nameNodePort(9020).format(true).build()
    miniDFSCluster.waitClusterUp()

    val r = miniDFSCluster.getConfiguration(0)
    miniDFSCluster.getFileSystem.mkdir(new Path("/tmp"), new FsPermission(777.toShort))

    val sparkConf = new SparkConf().setMaster(master).setAppName("test")
    _sc = new SparkContext(sparkConf)
  }

  def shutdown() {
    if (_sc != null) {
      _sc.stop()
    }

    if (yarnCluster != null) {
      yarnCluster.stop()
    }

    System.clearProperty("SPARK_YARN_MODE")
    _sc = null
    yarnCluster = null
  }

  override def afterAll() {
    shutdown()
    super.afterAll()
  }
}
