package com.holdenkarau.spark.testing

import org.apache.spark.sql.types.DataType
import org.apache.spark.ml.linalg.SQLDataTypes.{MatrixType, VectorType}
import org.apache.spark.ml.linalg.{DenseMatrix, Vectors}
import org.scalacheck.{Arbitrary, Gen}

/**
 * Extractor that matches the UDTs exposed by Spark ML.
 */
object MLUserDefinedType {
  def unapply(dataType: DataType): Option[Gen[Any]] =
    dataType match {
      case MatrixType => {
        val dense = for {
          rows <- Gen.choose(0, 20)
          cols <- Gen.choose(0, 20)
          values <- Gen.containerOfN[Array, Double](rows * cols, Arbitrary.arbitrary[Double])
        } yield new DenseMatrix(rows, cols, values)
        val sparse = dense.map(_.toSparse)
        Some(Gen.oneOf(dense, sparse))
      }
      case VectorType => {
        val dense = Arbitrary.arbitrary[Array[Double]].map(Vectors.dense)
        val sparse = for {
          indices <- Gen.nonEmptyContainerOf[Set, Int](Gen.choose(0, Int.MaxValue - 1))
          values <- Gen.listOfN(indices.size, Arbitrary.arbitrary[Double])
        } yield Vectors.sparse(indices.max + 1, indices.toSeq.zip(values))
        Some(Gen.oneOf(dense, sparse))
      }
      case _ => None
    }
}
