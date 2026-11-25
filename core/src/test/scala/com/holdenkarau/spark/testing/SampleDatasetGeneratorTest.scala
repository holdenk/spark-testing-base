package com.holdenkarau.spark.testing

import org.apache.spark.sql.{Dataset, SQLContext}
import org.scalacheck.{Gen, Arbitrary}
import org.scalacheck.Prop.forAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.Checkers
import org.apache.spark.sql.SparkSession

class SampleDatasetGeneratorTest extends AnyFunSuite
    with SharedSparkContext with Checkers {

  test("test generating Datasets[String]") {
    val sqlContext = SparkSession.builder.getOrCreate().sqlContext
    import sqlContext.implicits._

    val property =
      forAll(
        DatasetGenerator.genDataset[String](sqlContext)(
          Arbitrary.arbitrary[String])) {
        dataset => dataset.map(_.length).count() == dataset.count()
      }

    check(property)
  }

  test("test generating sized Datasets[String]") {
    val sqlContext = SparkSession.builder.getOrCreate().sqlContext
    import sqlContext.implicits._

    val property =
      forAll {
        DatasetGenerator.genSizedDataset[(Int, String)](sqlContext) { size =>
          Gen.listOfN(size, Arbitrary.arbitrary[Char]).map(l => (size, l.mkString))
        }
      }{
        dataset =>
          val tuples = dataset.collect()
          val value = dataset.map{ case (_, str) => str.length}
          tuples.forall{ case (size, str) => size == str.length} &&
          value.count() == dataset.count
      }

    check(property)
  }

  test("test generating Datasets[Custom Class]") {
    val sqlContext = SparkSession.builder.getOrCreate().sqlContext
    import sqlContext.implicits._

    val carGen: Gen[Dataset[Car]] =
      DatasetGenerator.genDataset[Car](sqlContext) {
        val generator: Gen[Car] = for {
          name <- Arbitrary.arbitrary[String]
          speed <- Arbitrary.arbitrary[Int]
        } yield (Car(name, speed))

        generator
    }

    val property =
      forAll(carGen) {
        dataset => dataset.map(_.speed).count() == dataset.count()
      }

    check(property)
  }
}

case class Car(name: String, speed: Int)
