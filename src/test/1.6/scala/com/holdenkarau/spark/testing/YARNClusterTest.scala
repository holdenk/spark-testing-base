package com.holdenkarau.spark.testing

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}


class YARNClusterTest extends FunSuite with BeforeAndAfterAll {
  var yarnCluster: YARNCluster = null
  var sc: SparkContext = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    yarnCluster = new YARNCluster()
    yarnCluster.startYARN()

    val sparkConf = new SparkConf()
      .setMaster("yarn-client")
      .setAppName("test")
    sc = new SparkContext(sparkConf)
  }

  test("test running simple count") {
    val rdd = sc.parallelize(List(1, 2, 3, 4))
    println("Count: " + rdd.count)
  }

  test("test save and load RDD") {
    val originalStr = "Hello, Hi, Hay, How are you ?"

    val strRDD = sc.parallelize(List(originalStr))
    val tmpDir = Utils.createTempDir()
    Utils.deleteRecursively(tmpDir)

    strRDD.saveAsTextFile(tmpDir.getAbsolutePath)

    val readStr = sc.textFile(tmpDir.getAbsolutePath).collect().headOption
    assert(readStr.isDefined)
    readStr.foreach(result => assert(result === originalStr))
  }

  override def afterAll(): Unit = {
    sc.stop()
    yarnCluster.shutdownYARN()
    super.afterAll()
  }
}
