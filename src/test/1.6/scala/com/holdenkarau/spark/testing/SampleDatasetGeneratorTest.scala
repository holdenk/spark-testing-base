package com.holdenkarau.spark.testing

import org.apache.spark.sql.{Dataset, SQLContext}
import org.scalacheck.{Gen, Arbitrary}
import org.scalacheck.Prop.forAll
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

class SampleDatasetGeneratorTest extends FunSuite
    with SharedSparkContext with Checkers {

  test("test generating Datasets[String]") {
    val sqlContext = new SQLContext(sc)
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
    val sqlContext = new SQLContext(sc)
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
    val sqlContext = new SQLContext(sc)
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

  test("test generating sized Datasets[Custom Class]") {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // In 2.3 List is fine, however in 2.0/2.1 the generator returns
    // a concrete sub type which isn't handled well.
    val carGen: Gen[Dataset[Seq[Car]]] =
      DatasetGenerator.genSizedDataset[Seq[Car]](sqlContext) { size =>
        val slowCarsTopNumber = math.ceil(size * 0.1).toInt
        def carGenerator(speed: Gen[Int]): Gen[Car] = for {
          name <- Arbitrary.arbitrary[String]
          speed <- speed
        } yield Car(name, speed)

        val cars: Gen[List[Car]] = for {
          slowCarsNumber: Int <- Gen.choose(0, slowCarsTopNumber)
          slowCars: List[Car] <- Gen.listOfN(slowCarsNumber, carGenerator(Gen.choose(0, 20)))
          normalSpeedCars: List[Car] <- Gen.listOfN(
            size - slowCarsNumber,
            carGenerator(Gen.choose(21, 150))
          )
        } yield {
          slowCars ++ normalSpeedCars
        }
        cars
      }

    val property =
      forAll(carGen.map(_.flatMap(identity))) {
        dataset =>
          val cars = dataset.collect()
          val dataSetSize  = cars.length
          val slowCars = cars.filter(_.speed < 21)
          slowCars.length <= dataSetSize * 0.1 &&
            cars.map(_.speed).length == dataSetSize
      }

    check(property)
  }
}

case class Car(name: String, speed: Int)
