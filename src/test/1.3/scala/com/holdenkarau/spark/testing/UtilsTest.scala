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

import java.io._
import java.nio.file.Files

import org.scalatest.FunSuite

class UtilsTest extends FunSuite {
  test("test utils cleanup") {
    val tempDir = Utils.createTempDir()
    val tempPath = tempDir.toPath().toAbsolutePath().toString()
    Utils.shutDownCleanUp()
    Thread.sleep(1)
    assert(!(new File(tempPath).exists()))
  }

  test("test utils cleanup with sub items") {
    val tempDir = Utils.createTempDir()
    val tempPath = tempDir.toPath().toAbsolutePath().toString()
    val f = new File(tempPath + "/magicpanda")
    val pw = new PrintWriter(f)
    pw.write("junk")
    pw.close()
    Files.createSymbolicLink(new File(tempPath +"/murh2").toPath, f.toPath)
    Utils.shutDownCleanUp()
    Thread.sleep(1)
    assert(!(new File(tempPath).exists()))
    assert(!(new File(f.toPath().toAbsolutePath().toString()).exists()))
  }
}
