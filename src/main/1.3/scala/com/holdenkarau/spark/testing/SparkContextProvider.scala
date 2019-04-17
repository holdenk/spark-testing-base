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
import org.apache.spark.SparkConf

trait SparkContextProvider {
  def sc: SparkContext

  def appID: String = (this.getClass.getName
    + math.floor(math.random * 10E4).toLong.toString)

  def conf = {
    new SparkConf().
      setMaster("local[*]").
      setAppName("test").
      set("spark.ui.enabled", "false").
      set("spark.app.id", appID).
      set("spark.driver.host", "localhost")
      set("spark.sql.shuffle.partitions", "1")
  }


  /**
   * Setup work to be called when creating a new SparkContext. Default implementation
   * currently sets a checkpoint directory.
   *
   * This _should_ be called by the context provider automatically.
   */
  def setup(sc: SparkContext): Unit = {
    sc.setCheckpointDir(Utils.createTempDir().toPath().toString)
  }
}
