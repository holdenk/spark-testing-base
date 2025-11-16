package com.holdenkarau.spark.testing

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoder, SQLContext, SparkSession}
import org.scalacheck.{Arbitrary, Gen}

import scala.reflect.ClassTag

object DatasetGenerator {

  /**
   * Generate an Dataset Generator of the desired type. Attempt to try different
   * number of partitions so as to catch problems with empty partitions, etc.
   * minPartitions defaults to 1, but when generating data too large for a single
   * machine, choose a larger value.
   *
   * @param spark         Spark Session
   * @param minPartitions minimum number of partitions
   * @param generator     used to create the generator. This function will be
   *                      used to create the generator as many times as required.
   * @return
   */
  def genDataset[T: ClassTag : Encoder]
    (spark: SparkSession, minPartitions: Int)
    (generator: => Gen[T]): Gen[Dataset[T]] = {
    arbitraryDataset(spark, minPartitions)(generator).arbitrary
  }

  /**
   * Generate an Dataset Generator of the desired type. Attempt to try different
   * number of partitions so as to catch problems with empty partitions, etc.
   * minPartitions defaults to 1, but when generating data too large for a single
   * machine, choose a larger value.
   *
   * @param sqlCtx        Spark sql Context
   * @param minPartitions defaults to 1
   * @param generator     used to create the generator. This function will be
   *                      used to create the generator as many times as required.
   * @return
   */
  def genDataset[T: ClassTag : Encoder]
    (sqlCtx: SQLContext, minPartitions: Int = 1)
    (generator: => Gen[T]): Gen[Dataset[T]] = {
    arbitraryDataset(sqlCtx, minPartitions)(generator).arbitrary
  }

  /**
    * Generate an Dataset Generator of the desired type with its size accessible.
    * Attempt to try different
    * number of partitions so as to catch problems with empty partitions, etc.
    * minPartitions defaults to 1, but when generating data too large for a single
    * machine, choose a larger value.
    *
    * @param spark         Spark Session
    * @param minPartitions minimum number of partitions
    * @param generator     used to create the generator. This function will be
    *                      used to create the generator as many times as required.
    * @return
    */
  def genSizedDataset[T: ClassTag : Encoder]
    (spark: SparkSession, minPartitions: Int)
    (generator: Int => Gen[T]): Gen[Dataset[T]] = {
    arbitrarySizedDataset(spark, minPartitions)(generator).arbitrary
  }

  /**
    * Generate an Dataset Generator of the desired type with its size accessible.
    * Attempt to try different
    * number of partitions so as to catch problems with empty partitions, etc.
    * minPartitions defaults to 1, but when generating data too large for a single
    * machine, choose a larger value.
    *
    * @param sqlCtx        Spark sql Context
    * @param minPartitions defaults to 1
    * @param generator     used to create the generator. This function will be
    *                      used to create the generator as many times as required.
    * @return
    */
  def genSizedDataset[T: ClassTag : Encoder]
    (sqlCtx: SQLContext, minPartitions: Int = 1)
    (generator: Int => Gen[T]): Gen[Dataset[T]] = {
    arbitrarySizedDataset(sqlCtx, minPartitions)(generator).arbitrary
  }

  /**
   * Generate an Dataset Generator of the desired type. Attempt to try different
   * number of partitions so as to catch problems with empty partitions, etc.
   * minPartitions defaults to 1, but when generating data too large for a single
   * machine, choose a larger value.
   *
   * @param spark         Spark Session
   * @param minPartitions minimum number of partitions
   * @param generator     used to create the generator. This function will be used to
   *                      create the generator as many times as required.
   * @return
   */
  def arbitraryDataset[T: ClassTag : Encoder]
    (spark: SparkSession, minPartitions: Int)
    (generator: => Gen[T]): Arbitrary[Dataset[T]] = {

    val rddGen: Gen[RDD[T]] =
      RDDGenerator.genRDD[T](spark.sparkContext, minPartitions)(generator)
    val datasetGen: Gen[Dataset[T]] =
      rddGen.map(rdd => spark.createDataset(rdd))

    Arbitrary {
      datasetGen
    }
  }

  /**
   * Generate an Dataset Generator of the desired type. Attempt to try different
   * number of partitions so as to catch problems with empty partitions, etc.
   * minPartitions defaults to 1, but when generating data too large for a single
   * machine, choose a larger value.
   *
   * @param sqlCtx        Spark sql Context
   * @param minPartitions defaults to 1
   * @param generator     used to create the generator. This function will be used to
   *                      create the generator as many times as required.
   * @return
   */
  def arbitraryDataset[T: ClassTag : Encoder]
    (sqlCtx: SQLContext, minPartitions: Int = 1)
    (generator: => Gen[T]): Arbitrary[Dataset[T]] = {

    val rddGen: Gen[RDD[T]] =
      RDDGenerator.genRDD[T](sqlCtx.sparkContext, minPartitions)(generator)
    val datasetGen: Gen[Dataset[T]] =
      rddGen.map(rdd => sqlCtx.createDataset(rdd))

    Arbitrary {
      datasetGen
    }
  }

  /**
    * Generate an Dataset Generator of the desired type. Attempt to try different
    * number of partitions so as to catch problems with empty partitions, etc.
    * minPartitions defaults to 1, but when generating data too large for a single
    * machine, choose a larger value.
    *
    * @param spark         Spark Session
    * @param minPartitions minimum number of partitions
    * @param generator     used to create the generator. This function will be used
    *                      to create the generator as many times as required.
    * @return
    */
  def arbitrarySizedDataset[T: ClassTag : Encoder]
    (spark: SparkSession, minPartitions: Int)
    (generator: Int => Gen[T]): Arbitrary[Dataset[T]] = {

    val rddGen: Gen[RDD[T]] =
      RDDGenerator.genSizedRDD[T](spark.sparkContext, minPartitions)(generator)
    val datasetGen: Gen[Dataset[T]] =
      rddGen.map(rdd => spark.createDataset(rdd))

    Arbitrary {
      datasetGen
    }
  }

  /**
    * Generate an Dataset Generator of the desired type. Attempt to try different
    * number of partitions so as to catch problems with empty partitions, etc.
    * minPartitions defaults to 1, but when generating data too large for a single
    * machine, choose a larger value.
    *
    * @param sqlCtx        Spark sql Context
    * @param minPartitions defaults to 1
    * @param generator     used to create the generator. This function will be used
    *                      to create the generator as many times as required.
    * @return
    */
  def arbitrarySizedDataset[T: ClassTag : Encoder]
    (sqlCtx: SQLContext, minPartitions: Int = 1)
    (generator: Int => Gen[T]): Arbitrary[Dataset[T]] = {

    val rddGen: Gen[RDD[T]] =
      RDDGenerator.genSizedRDD[T](sqlCtx.sparkContext, minPartitions)(generator)
    val datasetGen: Gen[Dataset[T]] =
      rddGen.map(rdd => sqlCtx.createDataset(rdd))

    Arbitrary {
      datasetGen
    }
  }
}
