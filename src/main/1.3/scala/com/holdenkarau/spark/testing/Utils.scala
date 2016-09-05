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

/*
 * This is a subset of the Spark Utils object that we use for testing
 */
package com.holdenkarau.spark.testing

import java.io._
import java.util.UUID
import java.net.BindException

import org.eclipse.jetty.util.MultiException
import org.apache.spark.{Logging, SparkConf, SparkException}

import scala.reflect.ClassTag


object Utils extends Logging {
  private val shutdownDeletePaths = new scala.collection.mutable.HashSet[String]()

  // Add a shutdown hook to delete the temp dirs when the JVM exits
  Runtime.getRuntime.addShutdownHook(new Thread("delete Spark temp dirs") {
    override def run(): Unit = {
      shutDownCleanUp()
    }
  })

  def shutDownCleanUp(): Unit = {
    shutdownDeletePaths.foreach(cleanupPath)
    shutdownDeletePaths.foreach(cleanupPath)
  }
  private def cleanupPath(dirPath: String) = {
    try {
      Utils.deleteRecursively(new File(dirPath))
    } catch {
      // Doesn't really matter if we fail.
      case e: Exception => println("Exception during cleanup")
    }
  }
  /**
    * Check to see if file is a symbolic link.
    */
  def isSymlink(file: File): Boolean = {
    if (file == null) throw new NullPointerException("File must not be null")
    val fileInCanonicalDir = if (file.getParent() == null) {
      file
    } else {
      new File(file.getParentFile().getCanonicalFile(), file.getName())
    }

    !fileInCanonicalDir.getCanonicalFile().equals(fileInCanonicalDir.getAbsoluteFile())
  }

  private def listFilesSafely(file: File): Seq[File] = {
    if (file.exists()) {
      val files = file.listFiles()
      if (files == null) {
        throw new IOException("Failed to list files for dir: " + file)
      }
      files
    } else {
      List()
    }
  }


  /**
    * Delete a file or directory and its contents recursively.
    * Don't follow directories if they are symlinks.
    * Throws an exception if deletion is unsuccessful.
    */
  def deleteRecursively(file: File) {
    if (file != null) {
      try {
        if (file.isDirectory && !isSymlink(file)) {
          var savedIOException: IOException = null
          for (child <- listFilesSafely(file)) {
            try {
              deleteRecursively(child)
            } catch {
              // In case of multiple exceptions, only last one will be thrown
              case ioe: IOException => savedIOException = ioe
            }
          }
          if (savedIOException != null) {
            throw savedIOException
          }
          shutdownDeletePaths.synchronized {
            shutdownDeletePaths.remove(file.getAbsolutePath)
          }
        }
      } finally {
        if (!file.delete()) {
          // Delete can also fail if the file simply did not exist
          if (file.exists()) {
            throw new IOException("Failed to delete: " + file.getAbsolutePath)
          }
        }
      }
    }
  }


  // Register the path to be deleted via shutdown hook
  def registerShutdownDeleteDir(file: File) {
    val absolutePath = file.getAbsolutePath()
    shutdownDeletePaths.synchronized {
      shutdownDeletePaths += absolutePath
    }
  }


  /**
    * Create a directory inside the given parent directory. The directory is guaranteed to be
    * newly created, and is not marked for automatic deletion.
    */
  def createDirectory(root: String): File = {
    var attempts = 0
    val maxAttempts = 10
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp directory (under " + root + ") after " +
          maxAttempts + " attempts!")
      }
      try {
        dir = new File(root, "spark-" + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        }
      } catch { case e: SecurityException => dir = null; }
    }

    dir
  }

  /**
    * Create a temporary directory inside the given parent directory. The directory will be
    * automatically deleted when the VM shuts down.
    */
  def createTempDir(root: String = System.getProperty("java.io.tmpdir")): File = {
    val dir = createDirectory(root)
    registerShutdownDeleteDir(dir)
    dir
  }


  /**
   * Attempt to start a service on the given port, or fail after a number of attempts.
   * Each subsequent attempt uses 1 + the port used in the previous attempt (unless the port is 0).
   *
   * @param startPort The initial port to start the service on.
   * @param startService Function to start service on a given port.
   *                     This is expected to throw java.net.BindException on port collision.
   * @param conf A SparkConf used to get the maximum number of retries when binding to a port.
   * @param serviceName Name of the service.
   */
  def startServiceOnPort[T](
      startPort: Int,
      startService: Int => (T, Int),
      conf: SparkConf,
      serviceName: String = ""): (T, Int) = {

    require(startPort == 0 || (1024 <= startPort && startPort < 65536),
      "startPort should be between 1024 and 65535 (inclusive), or 0 for a random free port.")

   val serviceString = if (serviceName.isEmpty) "" else s" '$serviceName'"
   val maxRetries = 100
   for (offset <- 0 to maxRetries) {
     // Do not increment port if startPort is 0, which is treated as a special port
     val tryPort = if (startPort == 0) {
       startPort
     } else {
       // If the new port wraps around, do not try a privilege port
       ((startPort + offset - 1024) % (65536 - 1024)) + 1024
     }
     try {
       val (service, port) = startService(tryPort)
       logInfo(s"Successfully started service$serviceString on port $port.")
       return (service, port)
     } catch {
       case e: Exception if isBindCollision(e) =>
         if (offset >= maxRetries) {
           val exceptionMessage =
             s"${e.getMessage}: Service$serviceString failed after $maxRetries retries!"
           val exception = new BindException(exceptionMessage)
           // restore original stack trace
           exception.setStackTrace(e.getStackTrace)
           throw exception
         }
         logWarning(s"Service$serviceString could not bind on port $tryPort. " +
           s"Attempting port ${tryPort + 1}.")
     }
   }
   // Should never happen
   throw new SparkException(s"Failed to start service$serviceString on port $startPort")
  }

  /**
   * Return whether the exception is caused by an address-port collision when binding.
   */
  def isBindCollision(exception: Throwable): Boolean = {
    import scala.collection.JavaConversions._

    exception match {
     case e: BindException =>
       if (e.getMessage != null) {
         return true
       }
       isBindCollision(e.getCause)
     case e: MultiException => e.getThrowables.exists(isBindCollision)
     case e: Exception => isBindCollision(e.getCause)
     case _ => false
   }
  }

  private[holdenkarau] def fakeClassTag[T]: ClassTag[T] = ClassTag.AnyRef.asInstanceOf[ClassTag[T]]
}
