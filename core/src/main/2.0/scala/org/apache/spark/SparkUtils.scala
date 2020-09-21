package org.apache.spark

import org.apache.spark.util.Utils

object SparkUtils {
  def clearLocalRootDirs(): Unit = {
    Utils.clearLocalRootDirs()
  }
}
