package com.holdenkarau.spark.testing

import org.apache.spark.sql.types.DataType
import org.scalacheck.Gen

/**
  * Compatibility shim for the extractor that matches the UDTs exposed by Spark ML.
  */
object MLUserDefinedType {
  def unapply(dataType: DataType): Option[Gen[Any]] =
    None
}
