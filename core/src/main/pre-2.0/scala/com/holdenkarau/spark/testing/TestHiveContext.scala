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

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.hive._
import org.apache.spark.sql.types.StructType
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars

private[testing] class TestHiveContext(sc: SparkContext, config: Map[String, String],
  localMetastorePath: String, localWarehousePath: String) extends HiveContext(sc) {
  setConf("javax.jdo.option.ConnectionURL",
    s"jdbc:derby:;databaseName=$localMetastorePath;create=true")
  setConf("hive.metastore.warehouse.dir", localWarehousePath)
  override def configure(): Map[String, String] = config
}
