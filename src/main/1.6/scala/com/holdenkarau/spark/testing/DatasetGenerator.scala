package com.holdenkarau.spark.testing

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoder, SQLContext}
import org.scalacheck.Gen

import scala.reflect.ClassTag

object DatasetGenerator {
  def genDataset[T : ClassTag : Encoder](sqlCtx: SQLContext, minPartitions: Int = 1)
                            (getGenerator: => Gen[T]): Gen[Dataset[T]] = {

    val rddGen: Gen[RDD[T]] = RDDGenerator.genRDD[T](sqlCtx.sparkContext, minPartitions)(getGenerator)
    val datasetGen: Gen[Dataset[T]] = rddGen.map(rdd => sqlCtx.createDataset(rdd))

    datasetGen
  }
}
