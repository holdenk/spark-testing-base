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

package com.holdenkarau.spark.testing.connect

import java.net.InetSocketAddress

// spark-connect 3.5 ships gRPC shaded under org.sparkproject, and only
// interceptors built against that copy can be registered with its server.
import org.sparkproject.connect.grpc.{Grpc, Metadata, ServerCall,
  ServerCallHandler, ServerInterceptor, Status}

/**
 * Refuses every call that does not come from a loopback address.
 *
 * Spark 3.5 has no `spark.connect.grpc.binding.address`, so the test server
 * cannot be told to listen on loopback only: it listens on every interface,
 * and Spark Connect does no authentication by default. This is the next best
 * thing -- the port is still open, but a call from anywhere else is closed
 * with PERMISSION_DENIED before it reaches Spark.
 *
 * Registered through `spark.connect.grpc.interceptor.classes`, which
 * instantiates it reflectively, hence a class with a no-arg constructor.
 */
class LoopbackOnlyInterceptor extends ServerInterceptor {
  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      next: ServerCallHandler[ReqT, RespT]): ServerCall.Listener[ReqT] = {
    call.getAttributes.get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR) match {
      case peer: InetSocketAddress
          if peer.getAddress != null && peer.getAddress.isLoopbackAddress =>
        next.startCall(call, headers)
      case peer =>
        call.close(
          Status.PERMISSION_DENIED.withDescription(
            s"${LoopbackOnlyInterceptor.Refusal} (peer: $peer)"),
          new Metadata())
        new ServerCall.Listener[ReqT] {}
    }
  }
}

object LoopbackOnlyInterceptor {
  val Refusal = "spark-testing-base's Connect test server only accepts " +
    "connections from localhost"
}
