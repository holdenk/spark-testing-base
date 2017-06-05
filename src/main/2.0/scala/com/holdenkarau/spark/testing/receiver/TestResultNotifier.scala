package com.holdenkarau.spark.testing.receiver

import java.io._
import java.nio.file.Path
import java.util.UUID

import org.apache.commons.io.FileUtils
import org.apache.spark.Logging
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

/**
  * Provides a mechanism that records -- into a temp directory in the local file
  * system -- the results of running user-provided code that generates a
  * [[org.apache.spark.streaming.dstream.DStream]], as well as a getResults()
  * method which returns a [[Future]] that provides access to a list of those
  * recorded results.
  *
  * Note that the temp directory will be deleted after getResults() is called.
  * We could consider replacing the heavyweight method of writing to files by
  * a lighter weight mechanism like broadcast variables in a subsequent
  * release (although our first attempt at this failed.)
  *
  * @param resultsDirPath - path of the directory where results will be written.
  *
  * @param numExpectedResults - the number of individual items that the code
  *                           under test is expected to write into
  *                           the [[org.apache.spark.streaming.dstream.DStream]]
  *                           it produces. Note that the value of this parameter will
  *                           most typically be calculated from the number of
  *                           elements in the list passed as
  *                           [[InputStreamTestingContext.dStreamCreationFunc()]].)
  *
  *
  * @tparam T - the type of object written into the DStream under test.
  */
class TestResultNotifier[T](val resultsDirPath: String,
                            val numExpectedResults: Int)
  extends Logging  with Serializable {

  private[receiver] val resultsDataFilePath: String =
    resultsDirPath + File.separator + "results.dat"
  private[receiver] val doneSentinelFilePath: String =
    resultsDirPath + File.separator + "results.done"

  def countOfResultsEqualsExpected(resultsDirPath: String): Boolean = {
    val foundList = new File(resultsDirPath).listFiles.filter{
      file =>
        println(s"${Thread.currentThread().getName} -- examinging: $file")
        file.isFile
    }.toList

    foundList.length >= numExpectedResults
  }

  /**
    * Use Java serialization to write out an item produced by  user-provided code
    * that generates a [[org.apache.spark.streaming.dstream.DStream]] to a
    * directory which will be monitored by the getResults method.
    *
    * @param result - the item to record
    */
  def recordResult(result: T): Unit = {
    val logger = LoggerFactory.getLogger(getClass)
    logInfo(s"TestResultNotifier recording results: $result")

    try {
      val fos = new FileOutputStream(
        new File (resultsDataFilePath +  UUID.randomUUID().toString))
      val oos = new ObjectOutputStream(fos);
      oos.writeObject(result)
      oos.close()
      if (countOfResultsEqualsExpected(resultsDirPath)) {
        new File (doneSentinelFilePath).mkdir()   // sentinel file: marks as 'done'
      }
    } catch {
      case  e: Throwable=>
        throw new RuntimeException(
          s"util.TestResultNotifier could not record result: $result", e)
    }
  }

  /**
    * Returns a Future which provides access to  a List of all items recorded by
    * the recordResult() method. The Future will be completed when the number
    * of items record reaches
    * [[TestResultNotifier.numExpectedResults]]. If more than this number of
    * items is written the extra items are not guaranteed to be present
    * in the Future[List] returned by this method.
    */
  def getResults : Future[List[T]] = {
    val promise = Promise[List[T]]()
    val sentinelFile = new File(doneSentinelFilePath)
    val logger = LoggerFactory.getLogger(getClass)

    Future {
      var results : List[T] =  null
      try {
        while (results == null) {
          if (sentinelFile.exists()) {
            val buffer = new ListBuffer[T]()
            val filesToCheck =
              new File(resultsDirPath).
                listFiles.
                filter { f => ! f.getName.endsWith("done") }.toList
            logInfo(s"checking files: $filesToCheck")
            for (file: File <- filesToCheck) {
              val inputStream = new FileInputStream(file.getAbsolutePath)
              val ois = new ObjectInputStream(inputStream)
              val result: T = ois.readObject().asInstanceOf[T]
              buffer += result
              ois.close()
            }
            results = buffer.toList
            logInfo(s"returning results: $results")
            FileUtils.deleteDirectory(new File(resultsDirPath));
          } else {
            logDebug(s"no sentinel file at $sentinelFile yet")
          }
          Thread.sleep(10)
        }
      } catch {
        case  e: Throwable=>
          logError(s"util.TestResultNotifier could get results", e)
          promise.failure(e)
      }

      promise.success(results)
    }
    promise.future
  }
}

/**
  * Factory for a TestResultNotifier which creates the temp directory into which results will be recorded, and
  * initializes the  TestResultNotifier to expect the given number of result items to be written into this directory.
  */

object TestResultNotifierFactory extends Logging {
  def getForNResults[T](numExpectedResults: Int): TestResultNotifier[T] = {
    var dir: Path = java.nio.file.Files.createTempDirectory("test-results")
    val path: String = dir.toAbsolutePath.toString
    logInfo(s"initializing TestResultNotifier pointed to this directory: $path")
    new TestResultNotifier[T](path, numExpectedResults)
  }
}
