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

import java.io.File
import java.util.Date

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

/** Shares a local `SparkContext` between all tests in a suite and closes it at the end. */
trait SharedSparkContext extends BeforeAndAfterAll with SparkContextProvider {
  self: Suite =>

  override def beforeAll() {
    _spark = SharedSparkContext.createSparkSession()
    super.beforeAll()
  }

  override def afterAll() {
    try {
      LocalSparkContext.stop(_spark)
      _spark = null
    } finally {
      super.afterAll()
    }
  }

  override val conf = SharedSparkContext.conf

  @transient private var _spark: SparkSession = _

  override lazy val sc: SparkContext = _spark.sparkContext
  lazy val spark: SparkSession = _spark
}

object SharedSparkContext {
  val appID = new Date().toString + math.floor(math.random * 10E4).toLong.toString

  val conf = new SparkConf().
    setMaster("local[*]").
    setAppName("test").
    set("spark.ui.enabled", "false").
    set("spark.app.id", appID)

  def createSparkSession():SparkSession = {
    /** Constructs a configuration for hive, where the metastore is located in a temp directory. */
    val tempDir = Utils.createTempDir()
    val localMetastorePath = new File(tempDir, "metastore").getCanonicalPath
    val localWarehousePath = new File(tempDir, "wharehouse").getCanonicalPath
    def newBuilder() = {
      val builder = SparkSession.builder()
      builder.config(SharedSparkContext.conf)
      // We have to mask all properties in hive-site.xml that relates to metastore data source
      // as we used a local metastore here.
      HiveConf.ConfVars.values().map(WrappedConfVar(_)).foreach { confvar =>
        if (confvar.varname.contains("datanucleus") || confvar.varname.contains("jdo")) {
          builder.config(confvar.varname, confvar.getDefaultExpr())
        }
      }
      builder.config("javax.jdo.option.ConnectionURL",
        s"jdbc:derby:;databaseName=$localMetastorePath;create=true")
      builder.config("datanucleus.rdbms.datastoreAdapterClassName",
        "org.datanucleus.store.rdbms.adapter.DerbyAdapter")
      builder.config(HiveConf.ConfVars.METASTOREURIS.varname, "")
      builder.config("spark.sql.streaming.checkpointLocation",
        Utils.createTempDir().toPath().toString)
      builder.config("spark.sql.warehouse.dir",
        localWarehousePath)
    }

    newBuilder().getOrCreate()
  }
}
