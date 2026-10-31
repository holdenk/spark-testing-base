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

/**
 * ConnectEnabled normally lets DataFrameSuiteBase.afterAll close the Connect
 * session, because closing it twice makes the client re-send ReleaseSession
 * over a shut-down channel and retry for ~10 minutes. But that call is skipped
 * when the suite reuses its context, so ConnectEnabled has to do the close
 * itself in exactly that case.
 *
 * The failure mode is a leak rather than an assertion, so what this suite
 * really checks is that teardown completes: if the two paths are ever both
 * taken, this hangs instead of failing.
 */
class SampleConnectReuseContextTest extends ScalaDataFrameSuiteBase
    with ConnectEnabled {

  override implicit def reuseContextIfPossible: Boolean = true

  test("assertions still work over Connect when reusing the context") {
    import spark.implicits._
    assert(isConnectSession)
    val df = Seq(("Alice", 30), ("Bob", 25)).toDF("name", "age")
    assertDataFrameEquals(df, df)
    assert(df.count() === 2)
  }
}
