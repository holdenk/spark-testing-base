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
package com.holdenkarau.spark.testing.kafka

import java.util.Properties

import scala.collection.JavaConversions._

import kafka.consumer.ConsumerConfig
import org.apache.spark.streaming.kafka.KafkaTestUtils
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

@RunWith(classOf[JUnitRunner])
class KafkaTestUtilsTest extends FunSuite with BeforeAndAfterAll {

  private var kafkaTestUtils: KafkaTestUtils = _

  override def beforeAll(): Unit = {
    kafkaTestUtils = new KafkaTestUtils
    kafkaTestUtils.setup()
  }

  override def afterAll(): Unit = if (kafkaTestUtils != null) {
    kafkaTestUtils.teardown()
    kafkaTestUtils = null
  }

  test("Kafka send and receive message") {
    val topic = "test-topic"
    val message = "HelloWorld!"
    kafkaTestUtils.createTopic(topic)
    kafkaTestUtils.sendMessages(topic, message.getBytes)

    val consumerProps = new Properties()
    consumerProps.put("zookeeper.connect", kafkaTestUtils.zkAddress)
    consumerProps.put("group.id", "test-group")
    consumerProps.put("flow-topic", topic)
    consumerProps.put("auto.offset.reset", "smallest")
    consumerProps.put("zookeeper.session.timeout.ms", "2000")
    consumerProps.put("zookeeper.connection.timeout.ms", "6000")
    consumerProps.put("zookeeper.sync.time.ms", "2000")
    consumerProps.put("auto.commit.interval.ms", "2000")

    val consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProps))

    try {
      val topicCountMap = Map(topic -> new Integer(1))
      val consumerMap = consumer.createMessageStreams(topicCountMap)
      val stream = consumerMap.get(topic).get(0)
      val it = stream.iterator()
      val mess = it.next
      assert(new String(mess.message().map(_.toChar)) === message)
    } finally {
      consumer.shutdown()
    }
  }

}
