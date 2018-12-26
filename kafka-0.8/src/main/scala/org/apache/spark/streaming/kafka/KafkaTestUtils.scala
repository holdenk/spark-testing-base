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
package org.apache.spark.streaming.kafka

import java.net.InetSocketAddress

import org.apache.spark.{Logging, SparkConf}
import kafka.admin.AdminUtils
import kafka.api.Request
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.serializer.DefaultEncoder

import java.util.Properties

import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}
import org.I0Itec.zkclient.ZkClient
import kafka.utils.{ZKStringSerializer, ZkUtils}
import org.apache.spark.streaming.Time
import java.util.concurrent.TimeoutException

import scala.annotation.tailrec
import scala.language.postfixOps
import scala.util.control.NonFatal

import com.holdenkarau.spark.testing.Utils

/**
 * This is a helper class for Kafka test suites. This has the functionality to set up
 * and tear down local Kafka servers, and to push data using Kafka producers.
 *
 */
class KafkaTestUtils extends Logging{

  // Zookeeper related configurations
  private val zkHost = "localhost"
  private var zkPort: Int = 0
  private val zkConnectionTimeout = 6000
  private val zkSessionTimeout = 6000

  private var zookeeper: EmbeddedZookeeper = _

  private var zkClient: ZkClient = _

  // Kafka broker related configurations
  private val brokerHost = "localhost"
  private var brokerPort = 9092
  private var brokerConf: KafkaConfig = _

  // Kafka broker server
  private var server: KafkaServer = _

  // Kafka producer
  private var producer: Producer[Array[Byte], Array[Byte]] = _

  // Flag to test whether the system is correctly started
  private var zkReady = false
  private var brokerReady = false

  def zkAddress: String = {
    assert(zkReady, "Zookeeper not setup yet or already torn down, cannot get zookeeper address")
    s"$zkHost:$zkPort"
  }

  def brokerAddress: String = {
    assert(brokerReady, "Kafka not setup yet or already torn down, cannot get broker address")
    s"$brokerHost:$brokerPort"
  }

  def zookeeperClient: ZkClient = {
    assert(zkReady, "Zookeeper not setup yet or already torn down, cannot get zookeeper client")
    Option(zkClient).getOrElse(
      throw new IllegalStateException("Zookeeper client is not yet initialized"))
  }

  private def brokerConfiguration: Properties = {
    val props = new Properties()
    props.put("broker.id", "0")
    props.put("host.name", "localhost")
    props.put("port", brokerPort.toString)
    props.put("log.dir", Utils.createTempDir().getAbsolutePath)
    props.put("zookeeper.connect", zkAddress)
    props.put("log.flush.interval.messages", "1")
    props.put("replica.socket.timeout.ms", "1500")
    props
  }

  private def producerConfiguration: Properties = {
    val props = new Properties()
    props.put("metadata.broker.list", brokerAddress)
    props.put("serializer.class", classOf[DefaultEncoder].getName)
    props
  }
  // Set up the Embedded Zookeeper server and get the proper Zookeeper port
  private def setupEmbeddedZookeeper(): Unit = {
    // Zookeeper server startup
    zookeeper = new EmbeddedZookeeper(s"$zkHost:$zkPort")
    // Get the actual zookeeper binding port
    zkPort = zookeeper.actualPort
    zkClient = new ZkClient(s"$zkHost:$zkPort", zkSessionTimeout, zkConnectionTimeout,
      ZKStringSerializer)
    zkReady = true
  }

  private def waitUntilMetadataIsPropagated(topic: String, partition: Int): Unit = {
    def isPropagated: Boolean = server.apis.metadataCache.getPartitionInfo(topic, partition) match {
      case Some(partitionState) =>
        val leaderAndInSyncReplicas = partitionState.leaderIsrAndControllerEpoch.leaderAndIsr

        ZkUtils.getLeaderForPartition(zkClient, topic, partition).isDefined &&
          Request.isValidBrokerId(leaderAndInSyncReplicas.leader) &&
          leaderAndInSyncReplicas.isr.nonEmpty

      case _ =>
        false
    }
    eventually(Time(10000), Time(100)) {
      assert(isPropagated, s"Partition [$topic, $partition] metadata not propagated after timeout")
    }
  }
  // Set up the Embedded Kafka server
  private def setupEmbeddedKafkaServer(): Unit = {
    assert(zkReady, "Zookeeper should be set up beforehand")

    // Kafka broker startup
    Utils.startServiceOnPort(brokerPort, port => {
      brokerPort = port
      brokerConf = new KafkaConfig(brokerConfiguration)
      server = new KafkaServer(brokerConf)
      server.startup()
      (server, port)
    }, new SparkConf(), "KafkaBroker")

    brokerReady = true
  }

  /** setup the whole embedded servers, including Zookeeper and Kafka brokers */
  def setup(): Unit = {
    setupEmbeddedZookeeper()
    setupEmbeddedKafkaServer()
  }

  def teardown(): Unit = {
    brokerReady = false
    zkReady = false

    if (producer != null) {
      producer.close()
      producer = null
    }

    if (server != null) {
      server.shutdown()
      server = null
    }

    if (zkClient != null) {
      zkClient.close()
      zkClient = null
    }

    if (zookeeper != null) {
      zookeeper.shutdown()
      zookeeper = null
    }
  }

  /** Create a Kafka topic and wait until it propagated to the whole cluster */
  def createTopic(topic: String): Unit = {
    AdminUtils.createTopic(zkClient, topic, 1, 1)
    // wait until metadata is propagated
    waitUntilMetadataIsPropagated(topic, 0)
  }

  /** Send the array of messages to the Kafka broker */
  def sendMessages(topic: String, message: Array[Byte]): Unit = {
    producer = new Producer[Array[Byte], Array[Byte]](new ProducerConfig(producerConfiguration))
    producer.send(new KeyedMessage[Array[Byte], Array[Byte]](topic, message ) )
    producer.close()
    producer = null
  }

  private class EmbeddedZookeeper(val zkConnect: String) {
    val snapshotDir = Utils.createTempDir()
    val logDir = Utils.createTempDir()

    val zookeeper = new ZooKeeperServer(snapshotDir, logDir, 500)
    val (ip, port) = {
      val splits = zkConnect.split(":")
      (splits(0), splits(1).toInt)
    }
    val factory = new NIOServerCnxnFactory()
    factory.configure(new InetSocketAddress(ip, port), 16)
    factory.startup(zookeeper)

    val actualPort = factory.getLocalPort

    def shutdown() {
      zookeeper.shutdown()
      factory.shutdown()
    }
  }

  // A simplified version of scalatest eventually, rewritten here to avoid adding extra test
  // dependency
  def eventually[T](timeout: Time, interval: Time)(func: => T): T = {
    def makeAttempt(): Either[Throwable, T] = {
      try {
        Right(func)
      } catch {
        case e if NonFatal(e) => Left(e)
      }
    }

    val startTime = System.currentTimeMillis()
    @tailrec
    def tryAgain(attempt: Int): T = {
      makeAttempt() match {
        case Right(result) => result
        case Left(e) =>
          val duration = System.currentTimeMillis() - startTime
          if (duration < timeout.milliseconds) {
            Thread.sleep(interval.milliseconds)
          } else {
            throw new TimeoutException(e.getMessage)
          }

          tryAgain(attempt + 1)
      }
    }

    tryAgain(1)
  }

}
