package com.holdenkarau.spark.testing

import java.sql.Timestamp
import java.time.Duration

import org.scalatest.funsuite.AnyFunSuite

/**
 * Test for assertDatasetEquals methods in DataFrameSuiteBase.
 * This tests the new assertDatasetEquals API which is an alias for assertDataFrameEquals
 * for Dataset[Row] (DataFrames). For strongly-typed Datasets, use DatasetSuiteBase.
 */
class DatasetEqualsTest extends AnyFunSuite with DataFrameSuiteBase {

  test("assertDatasetEquals works with Dataset[Row] (DataFrame)") {
    import sqlContext.implicits._
    val list = List(TestPerson("Holden", 2000, 60.0), TestPerson("Hanafy", 23, 80.0))
    val dataset1 = sc.parallelize(list).toDF
    val dataset2 = sc.parallelize(list).toDF

    // Should work since they're equal
    assertDatasetEquals(dataset1, dataset2)
  }

  test("assertDatasetEquals fails with unequal Dataset[Row]") {
    import sqlContext.implicits._
    val list1 = List(TestPerson("Holden", 2000, 60.0), TestPerson("Hanafy", 23, 80.0))
    val list2 = List(TestPerson("Holden", 2000, 60.0), TestPerson("Hanafy", 23, 80.1))
    val dataset1 = sc.parallelize(list1).toDF
    val dataset2 = sc.parallelize(list2).toDF

    // Should fail since they're not equal
    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDatasetEquals(dataset1, dataset2)
    }
  }

  test("assertDatasetApproximateEquals works with Dataset[Row]") {
    import sqlContext.implicits._
    val list1 = List(TestPerson("Holden", 2000, 60.0), TestPerson("Hanafy", 23, 80.0))
    val list2 = List(TestPerson("Holden", 2000, 60.1), TestPerson("Hanafy", 23, 80.1))
    val dataset1 = sc.parallelize(list1).toDF
    val dataset2 = sc.parallelize(list2).toDF

    // Should work with tolerance
    assertDatasetApproximateEquals(dataset1, dataset2, 0.2, Duration.ZERO, _.show())
  }

  test("assertDatasetApproximateEquals fails with low tolerance on Dataset[Row]") {
    import sqlContext.implicits._
    val list1 = List(TestPerson("Holden", 2000, 60.0), TestPerson("Hanafy", 23, 80.0))
    val list2 = List(TestPerson("Holden", 2000, 60.5), TestPerson("Hanafy", 23, 80.0))
    val dataset1 = sc.parallelize(list1).toDF
    val dataset2 = sc.parallelize(list2).toDF

    // Should fail with low tolerance
    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDatasetApproximateEquals(dataset1, dataset2, 0.1, Duration.ZERO, _.show())
    }
  }

  test("assertDatasetApproximateEquals with timestamp tolerance") {
    import sqlContext.implicits._
    val list1 = List(TestMagicTime("Holden", Timestamp.valueOf("2018-01-12 20:41:32")))
    val list2 = List(TestMagicTime("Holden", Timestamp.valueOf("2018-01-12 20:41:49")))

    val dataset1 = sc.parallelize(list1).toDF
    val dataset2 = sc.parallelize(list2).toDF

    // Should work with timestamp tolerance
    assertDatasetApproximateEquals(dataset1, dataset2, 0.0, Duration.ofSeconds(20), _.show())

    // Should fail with low timestamp tolerance
    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDatasetApproximateEquals(dataset1, dataset2, 0.0, Duration.ofSeconds(10), _.show())
    }
  }

  test("assertDatasetEquals with empty datasets") {
    import sqlContext.implicits._
    val emptyDS1 = sc.parallelize(List[TestPerson]()).toDF
    val emptyDS2 = sc.parallelize(List[TestPerson]()).toDF

    // Should work with empty datasets
    assertDatasetEquals(emptyDS1, emptyDS2)
  }
}

case class TestPerson(name: String, age: Int, weight: Double)
case class TestMagicTime(name: String, t: Timestamp)
