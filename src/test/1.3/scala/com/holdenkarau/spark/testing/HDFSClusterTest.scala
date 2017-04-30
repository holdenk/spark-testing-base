package com.holdenkarau.spark.testing

import java.io.{
  BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

class HDFSClusterTest extends FunSuite with SharedSparkContext with RDDComparisons {

  var hdfsCluster: HDFSCluster = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    hdfsCluster = new HDFSCluster
    hdfsCluster.startHDFS()
  }

  test("get the namenode uri") {
    val nameNodeURI = hdfsCluster.getNameNodeURI()
    assert(nameNodeURI == "hdfs://localhost:8020")
  }

  test("read and write from spark to hdfs") {
    val list = List(1, 2, 3, 4, 5)
    val numRDD: RDD[Int] = sc.parallelize(list)

    val path = hdfsCluster.getNameNodeURI() + "/myRDD"
    numRDD.saveAsTextFile(path)

    val loadedRDD: RDD[Int] = sc.textFile(path).map(_.toInt)
    assertRDDEquals(numRDD, loadedRDD)
  }

  test("test creating local file to hdfs") {
    val path = new Path(hdfsCluster.getNameNodeURI() + "/myfile")
    val fs = FileSystem.get(path.toUri, new Configuration())

    val writer = new BufferedWriter(new OutputStreamWriter(fs.create(path)))
    val writtenString = "hello, it's me"
    writer.write(writtenString)
    writer.close()

    val reader = new BufferedReader(new InputStreamReader(fs.open(path)))
    val readString = reader.readLine()
    reader.close()

    assert(writtenString == readString)
  }

  override def afterAll() {
    hdfsCluster.shutdownHDFS()
    super.afterAll()
  }
}
