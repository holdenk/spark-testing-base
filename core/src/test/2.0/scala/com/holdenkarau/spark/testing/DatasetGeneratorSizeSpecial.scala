package com.holdenkarau.spark.testing

import org.apache.spark.sql.{Dataset, SQLContext}
import org.scalacheck.{Gen, Arbitrary}
import org.scalacheck.Prop.forAll
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers
import org.apache.spark.sql.SparkSession

class DatasetGeneratorSizeSpecial extends FunSuite
    with SharedSparkContext with Checkers {

  test("test generating sized Datasets[Custom Class]") {
    val sqlContext = SparkSession.builder.getOrCreate().sqlContext
    import sqlContext.implicits._

    // In 2.3 List is fine, however prior to 2.1 the generator returns
    // a concrete sub type which isn't handled well.
    // This works in 1.6.1+ but we only test in 2.0+ because that's easier
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
