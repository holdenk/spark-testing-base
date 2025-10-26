package org.apache.spark.sql

import org.apache.spark.sql.SparkSession
object EvilSessionTools {
  def extractSQLContext(session: SparkSession): SQLContext = {
    SparkSession.builder.getOrCreate().sqlContext
  }
}
