package com.holdenkarau.spark.testing

import org.apache.spark.sql._

import org.scalatest.FunSuite

class StructuredStreamingTests
    extends FunSuite with SharedSparkContext with StructuredStreamingBase {
  // re-use the spark context
  override implicit def reuseContextIfPossible: Boolean = true

  test("add 3") {
    import spark.implicits._
    val input = List(List(1), List(2, 3))
    val expected = List(4, 5, 6)
    def compute(input: Dataset[Int]): Dataset[Int] = {
      input.map(elem => elem + 3)
    }
    testSimpleStreamEndState(spark, input, expected, "append", compute)
  }

  test("stringify") {
    import spark.implicits._
    val input = List(List(1), List(2, 3))
    val expected = List("1", "2", "3")
    def compute(input: Dataset[Int]): Dataset[String] = {
      input.map(elem => elem.toString)
    }
    testSimpleStreamEndState(spark, input, expected, "append", compute)
  }
}
