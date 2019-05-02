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

import java.io.{File, FileOutputStream, OutputStreamWriter}
import java.net.URLClassLoader
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.google.common.base.Charsets.UTF_8
import com.google.common.io.Files
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.api.records.NodeState
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.MiniYARNCluster

import scala.collection.JavaConversions._

/**
 * Shares an HDFS MiniCluster based `SparkContext` between all tests in a suite and
 * closes it at the end. This requires that the env variable SPARK_HOME is set.
 * Further more if this is used prior to Spark 1.6.3,
 *  all Spark tests must run against the yarn mini cluster
 * (see https://issues.apache.org/jira/browse/SPARK-10812 for details).
 */
class YARNCluster extends YARNClusterLike

trait YARNClusterLike {
  // log4j configuration for the YARN containers, so that their output is collected
  // by YARN instead of trying to overwrite unit-tests.log.
  private val LOG4J_CONF =
    """
      |log4j.rootCategory=DEBUG, console
      |log4j.appender.console=org.apache.log4j.ConsoleAppender
      |log4j.appender.console.target=System.err
      |log4j.appender.console.layout=org.apache.log4j.PatternLayout
      |log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n
    """.stripMargin

  private val configurationFilePath = new File(
    this.getClass.getProtectionDomain().getCodeSource().getLocation().getPath())
    .getParentFile.getAbsolutePath + "/hadoop-site.xml"

  @transient private var yarnCluster: Option[MiniYARNCluster] = None
  private var tempDir: File = _
  private var logConfDir: File = _

  def startYARN() {
    tempDir = Utils.createTempDir()
    logConfDir = new File(tempDir, "log4j")
    logConfDir.mkdir()
    System.setProperty("SPARK_YARN_MODE", "true")

    val logConfFile = new File(logConfDir, "log4j.properties")
    Files.write(LOG4J_CONF, logConfFile, UTF_8)

    val yarnConf = new YarnConfiguration()
    // Disable the disk utilization check to avoid the test hanging when people's disks are
    // getting full.
    yarnConf.set("yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage",
      "100.0")

    val nodes = 2
    yarnCluster = Some(new MiniYARNCluster(getClass().getName(), nodes, 1, 1))
    yarnCluster.foreach(_.init(yarnConf))
    yarnCluster.foreach(_.start())

    val config = yarnCluster.map(_.getConfig()).get
    val deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10)
    while (config.get(YarnConfiguration.RM_ADDRESS).split(":")(1) == "0") {
      if (System.currentTimeMillis() > deadline) {
        throw new IllegalStateException("Timed out waiting for RM to come up.")
      }
      TimeUnit.MILLISECONDS.sleep(100)
    }

    val yarnClient = YarnClient.createYarnClient()
    yarnClient.init(config)
    yarnClient.start()

    val nodeReports = yarnClient.getNodeReports(NodeState.RUNNING)
    println(s"node reports in running: ${nodeReports}")

    val props = setupSparkProperties()
    val propsFile = File.createTempFile("spark", ".properties", tempDir)
    val writer = new OutputStreamWriter(new FileOutputStream(propsFile), UTF_8)
    props.store(writer, "Spark properties.")
    writer.close()
  }

  def setupSparkProperties() = {
    // Find the spark assembly jar
    // TODO: Better error messaging

    val sparkAssemblyJar = getAssemblyJar()
    val sparkAssemblyPath = getSparkAssemblyPath()

    // Set some yarn props
    sparkAssemblyJar.foreach{jar =>
      sys.props += ("spark.yarn.jar" -> ("local:" + jar))}
    sparkAssemblyPath.foreach{path =>
      sys.props += ("spark.yarn.jars" -> ("local:" + path + "*"))}
    sys.props += ("spark.executor.instances" -> "1")
    // Figure out our class path
    val childClasspath = generateClassPath()
    sys.props += ("spark.driver.extraClassPath" -> childClasspath)
    sys.props += ("spark.executor.extraClassPath" -> childClasspath)
    val sparkHome = sys.env("SPARK_HOME")

    // Handle the Spark JARS
    sys.props += ("spark.yarn.jars" -> s"${sparkHome}/jars/*.jar")
    val configurationFile = new File(configurationFilePath)
    if (configurationFile.exists()) {
      configurationFile.delete()
    }
    val configuration = yarnCluster.map(_.getConfig)
    configuration.foreach{config =>
      iterableAsScalaIterable(config).foreach { e =>
        sys.props += ("spark.hadoop." + e.getKey() -> e.getValue())
      }
      config.writeXml(new FileOutputStream(configurationFile))
    }
    // Copy the system props
    val props = new Properties()
    sys.props.foreach { case (k, v) =>
      if (k.startsWith("spark.")) {
        props.setProperty(k, v)
      }
    }
    props
  }

  // For old style (pre 2.0)
  def getAssemblyJar(): Option[String] = {
    val sparkAssemblyDirs = List(
      sys.env("SPARK_HOME") + "/assembly/target/scala-2.10/",
      sys.env("SPARK_HOME") + "/assembly/target/scala-2.11/")

    val sparkLibDir = List(sys.env("SPARK_HOME") + "/lib/")

    val candidateDirs = sparkAssemblyDirs ++ sparkLibDir
    val candidates = candidateDirs.map(
      dir => new File(dir)).filter(_.exists()).flatMap(_.listFiles)

    val sparkAssemblyJar = candidates.find { f =>
      val name = f.getName
      name.endsWith(".jar") && name.startsWith("spark-assembly")
    }.map(f => f.getAbsolutePath())

    sparkAssemblyJar
  }

  def getSparkAssemblyPath(): Option[String] = {
    val sparkAssemblyDirs = List(
      sys.env("SPARK_HOME") + "/assembly/target/scala-2.10/",
      sys.env("SPARK_HOME") + "/assembly/target/scala-2.11/")

    val sparkLibDir = List(sys.env("SPARK_HOME") + "/lib/")

    val candidateDirs = sparkAssemblyDirs ++ sparkLibDir
    val candidates = candidateDirs.filter(dir => new File(dir).exists())
    candidates.headOption
  }

  def generateClassPath(): String = {
    // Class path
    val clList =
      (List(logConfDir.getAbsolutePath(), sys.props("java.class.path")) ++
        classPathFromCurrentClassLoader ++ extraClassPath)
    val clPath = clList.mkString(File.pathSeparator)
    clPath
  }

  // Class path based on current env + program specific class path.
  def classPathFromCurrentClassLoader(): Seq[String] = {
    // This _assumes_ that either the current class loader or
    // parent class loader is a URLClassLoader
    val urlClassLoader = Thread.currentThread().getContextClassLoader() match {
      case uc: URLClassLoader => uc
      case xy => xy.getParent.asInstanceOf[URLClassLoader]
    }
    urlClassLoader.getURLs().toSeq.map(u => new File(u.toURI()).getAbsolutePath())
  }

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
    ).map(x => Option(x.getAbsolutePath)).flatMap(x => x)
  }

  def shutdownYARN() {
    yarnCluster.foreach(_.stop())
    System.clearProperty("SPARK_YARN_MODE")
    yarnCluster = None
  }

}
