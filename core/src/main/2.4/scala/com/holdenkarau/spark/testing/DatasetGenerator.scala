package com.holdenkarau.spark.testing

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoder, SparkSession, SQLContext}
import org.scalacheck.{Arbitrary, Gen}

import scala.reflect.ClassTag

object DatasetGenerator {

  /**
   * SparkSession-based overloads of the generators below.
   *
   * Note: these build their data as RDDs, so they do not work over Spark
   * Connect -- they are here because SQLContext is the deprecated API, not
   * because they are Connect-safe.
   *
   * None of them give `minPartitions` a default value: Scala only allows one
   * overloaded alternative to define defaults, and the SQLContext versions
   * already do.
   */
  def genDataset[T: ClassTag : Encoder]
    (spark: SparkSession, minPartitions: Int)
    (generator: => Gen[T]): Gen[Dataset[T]] = {
    genDataset(spark.sqlContext, minPartitions)(generator)
  }

  /** See [[genDataset]]; generates Datasets whose size is accessible. */
  def genSizedDataset[T: ClassTag : Encoder]
    (spark: SparkSession, minPartitions: Int)
    (generator: Int => Gen[T]): Gen[Dataset[T]] = {
    genSizedDataset(spark.sqlContext, minPartitions)(generator)
  }

  /** See [[genDataset]]; returns an Arbitrary rather than a Gen. */
  def arbitraryDataset[T: ClassTag : Encoder]
    (spark: SparkSession, minPartitions: Int)
    (generator: => Gen[T]): Arbitrary[Dataset[T]] = {
    arbitraryDataset(spark.sqlContext, minPartitions)(generator)
  }

  /** See [[genSizedDataset]]; returns an Arbitrary rather than a Gen. */
  def arbitrarySizedDataset[T: ClassTag : Encoder]
    (spark: SparkSession, minPartitions: Int)
    (generator: Int => Gen[T]): Arbitrary[Dataset[T]] = {
    arbitrarySizedDataset(spark.sqlContext, minPartitions)(generator)
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
