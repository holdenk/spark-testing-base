package com.holdenkarau.spark.testing

import org.apache.spark.sql.DataFrame
import org.scalacheck.util.Pretty

trait Prettify {

  implicit def prettyDataFrame(dataFrame: DataFrame): Pretty =
    Pretty { _ => describeDataframe(dataFrame)}

  private def describeDataframe(dataframe: DataFrame) =
    s"""<DataFrame: schema = ${dataframe.toString}, size = ${dataframe.count()},
        |values = (${dataframe.take(100).map(_.toString).mkString(", ")})>""".stripMargin.replace("\n", " ")
}

object Prettify extends Prettify
