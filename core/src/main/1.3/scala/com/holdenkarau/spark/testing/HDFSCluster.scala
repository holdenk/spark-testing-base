package com.holdenkarau.spark.testing

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.test.PathUtils

class HDFSCluster extends HDFSClusterLike

trait HDFSClusterLike {
  @transient private var hdfsCluster: MiniDFSCluster = null

  def startHDFS(): Unit = {
    println("Starting HDFS Cluster...")
    val baseDir = new File(PathUtils.getTestDir(getClass()), "miniHDFS")

    val conf = new Configuration()
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath())

    val builder = new MiniDFSCluster.Builder(conf)
    hdfsCluster = builder.nameNodePort(8020).format(true).build()
    hdfsCluster.waitClusterUp()
  }

  def getNameNodeURI(): String = {
    "hdfs://localhost:" + hdfsCluster.getNameNodePort()
  }

  def shutdownHDFS(): Unit = {
    hdfsCluster.shutdown()
  }
}