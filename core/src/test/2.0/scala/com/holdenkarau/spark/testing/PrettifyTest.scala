package com.holdenkarau.spark.testing

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalacheck.Gen
import org.scalacheck.Prop._
import org.scalacheck.util.Pretty
import org.scalatest.exceptions.GeneratorDrivenPropertyCheckFailedException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.Checkers
import org.apache.spark.sql.SparkSession

class PrettifyTest extends AnyFunSuite with SharedSparkContext with Checkers with Prettify {
  implicit val propertyCheckConfig = PropertyCheckConfiguration(minSize = 2, sizeRange = 0)

  test("pretty output of DataFrame's check") {
    val schema = StructType(List(StructField("name", StringType), StructField("age", IntegerType)))
    val sqlContext = SparkSession.builder.getOrCreate().sqlContext
    val nameGenerator = new ColumnGenerator("name", Gen.const("Holden Hanafy"))
    val ageGenerator = new ColumnGenerator("age", Gen.const(20))

    val dataframeGen = DataFrameGenerator.arbitraryDataFrameWithCustomFields(sqlContext, schema)(nameGenerator, ageGenerator)

    val actual = runFailingCheck(dataframeGen.arbitrary)
    val expected =
      Some("arg0 = <DataFrame: schema = [name: string, age: int], size = 2, values = ([Holden Hanafy,20], [Holden Hanafy,20])>")
    assert(actual == expected)
  }

  test("pretty output of RDD's check") {
    val rddGen = RDDGenerator.genRDD[(String, Int)](sc) {
      for {
        name <- Gen.const("Holden Hanafy")
        age <- Gen.const(20)
      } yield name -> age
    }

    val actual = runFailingCheck(rddGen)
    val expected =
      Some("""arg0 = <RDD: size = 2, values = ((Holden Hanafy,20), (Holden Hanafy,20))>""")
    assert(actual == expected)
  }

  test("pretty output of Dataset's check") {
    val sqlContext = SparkSession.builder.getOrCreate().sqlContext
    import sqlContext.implicits._

    val datasetGen = DatasetGenerator.genDataset[(String, Int)](sqlContext) {
      for {
        name <- Gen.const("Holden Hanafy")
        age <- Gen.const(20)
      } yield name -> age
    }

    val actual = runFailingCheck(datasetGen)
    val expected =
      Some("""arg0 = <Dataset: schema = [_1: string, _2: int], size = 2, values = ((Holden Hanafy,20), (Holden Hanafy,20))>""")
    assert(actual == expected)
  }

  private def runFailingCheck[T](genUnderTest: Gen[T])(implicit p: T => Pretty) = {
    val property = forAll(genUnderTest)(_ => false)
    val e = intercept[GeneratorDrivenPropertyCheckFailedException] {
      check(property)
    }
    takeSecondToLastLine(e.message)
  }

  private def takeSecondToLastLine(msg: Option[String]) =
    msg.flatMap(_.split("\n").toList.reverse.tail.headOption.map(_.trim))

}
