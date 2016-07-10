package com.holdenkarau.spark.testing

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalacheck.Gen
import org.scalacheck.Prop._
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

class PrettifyTest extends FunSuite with SharedSparkContext with Checkers with Prettify {

  test("pretty output of Dataframe's check") {
    implicit val generatorDrivenConfig =
      PropertyCheckConfig(minSize = 2, maxSize = 2)
    val schema = StructType(List(StructField("name", StringType), StructField("age", IntegerType)))
    val sqlContext = new SQLContext(sc)
    val nameGenerator = new ColumnGenerator("name", Gen.const("Holden Hanafy"))
    val ageGenerator = new ColumnGenerator("age", Gen.const(20))

    val dataframeGen = DataframeGenerator.arbitraryDataFrameWithCustomFields(sqlContext, schema)(nameGenerator, ageGenerator)

    val property =
      forAll(dataframeGen.arbitrary) {
        dataframe => false
      }
    val e = intercept[org.scalatest.exceptions.GeneratorDrivenPropertyCheckFailedException] {
      check(property)
    }
    val expected =
      Some("arg0 = <DataFrame: schema = [name: string, age: int], size = 2, values = ([Holden Hanafy,20], [Holden Hanafy,20])>")
    assert(takeSecondToLastLine(e.message) == expected)
  }

  private def takeSecondToLastLine(msg: Option[String]) =
    msg.flatMap(_.split("\n").toList.reverse.tail.headOption.map(_.trim))

}
