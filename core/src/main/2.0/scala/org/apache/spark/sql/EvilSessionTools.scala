package org.apache.spark.sql

object EvilSessionTools {
  def extractSQLContext(session: SparkSession): SQLContext = {
    new SQLContext(session)
  }
}
