package com.holdenkarau.spark.testing

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalacheck.Gen
import org.scalacheck.Prop._
import org.scalacheck.util.Pretty
import org.scalatest.FunSuite
import org.scalatest.exceptions.GeneratorDrivenPropertyCheckFailedException
import org.scalatest.prop.Checkers

class PrettifyTest extends FunSuite with SharedSparkContext with Checkers with Prettify {
  implicit val propertyCheckConfig = PropertyCheckConfig(minSize = 2, maxSize = 2)

  test("pretty output of Dataframe's check") {
    val schema = StructType(List(StructField("name", StringType), StructField("age", IntegerType)))
    val sqlContext = new SQLContext(sc)
    val nameGenerator = new ColumnGenerator("name", Gen.const("Holden Hanafy"))
    val ageGenerator = new ColumnGenerator("age", Gen.const(20))

    val dataframeGen = DataframeGenerator.arbitraryDataFrameWithCustomFields(sqlContext, schema)(nameGenerator, ageGenerator)

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
      Some("""arg0 = <RDD: size = 2, values = ("(Holden Hanafy,20)", "(Holden Hanafy,20)")>""")
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
