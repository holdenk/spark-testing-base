package com.holdenkarau.spark.testing

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}
import org.scalacheck.util.Pretty

trait Prettify {
  val maxNumberOfShownValues = 100

  implicit def prettyDataFrame(dataframe: DataFrame): Pretty =
    Pretty { _ => describeDataframe(dataframe)}

  implicit def prettyRDD(rdd: RDD[_]): Pretty =
    Pretty { _ => describeRDD(rdd)}

  implicit def prettyDataset(dataset: Dataset[_]): Pretty =
    Pretty { _ => describeDataset(dataset)}

  private def describeDataframe(dataframe: DataFrame) =
    s"""<DataFrame: schema = ${dataframe.toString}, size = ${dataframe.count()},
        |values = (${dataframe.take(maxNumberOfShownValues).mkString(", ")})>""".
      stripMargin.replace("\n", " ")

  private def describeRDD(rdd: RDD[_]) =
    s"""<RDD: size = ${rdd.count()},
        |values = (${rdd.take(maxNumberOfShownValues).mkString(", ")})>""".
      stripMargin.replace("\n", " ")

  private def describeDataset(dataset: Dataset[_]) =
    s"""<Dataset: schema = ${dataset.toString}, size = ${dataset.count()},
        |values = (${dataset.take(maxNumberOfShownValues).mkString(", ")})>""".
      stripMargin.replace("\n", " ")
}

object Prettify extends Prettify
