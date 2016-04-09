package com.holdenkarau.spark.testing

import org.scalatest.FunSuiteLike

import scala.reflect.ClassTag

trait TestSuite extends TestSuiteLike with FunSuiteLike {

  override def assertEmpty[U](arr: Array[U])(implicit CT: ClassTag[U]) =
    org.scalatest.Assertions.assert(arr.isEmpty)

  override def assert[U](expected: U, actual: U)(implicit CT: ClassTag[U]) =
    org.scalatest.Assertions.assert(expected === actual)

  override def assertTrue(expected: Boolean) =
    org.scalatest.Assertions.assert(expected === true)

  def assert[U](message: String, expected: U, actual: U)(implicit CT: ClassTag[U]) =
    org.scalatest.Assertions.assert(expected === actual, message)
}

trait JavaTestSuite extends TestSuiteLike {
  override def assertEmpty[U](arr: Array[U])(implicit CT: ClassTag[U]) = {
    if (!arr.isEmpty)
      throw new AssertionError("Not Equal Sample: " + arr.mkString(", "))
  }

  override def assert[U](expected: U, actual: U)(implicit CT: ClassTag[U]) =
    org.junit.Assert.assertEquals(expected, actual)

  override def assertTrue(expected: Boolean) =
    org.junit.Assert.assertTrue(expected)

  def assert[U](message: String, expected: U, actual: U)(implicit CT: ClassTag[U]) =
    org.junit.Assert.assertEquals(message, expected, actual)
}

trait TestSuiteLike {
  def assertEmpty[U](arr: Array[U])(implicit CT: ClassTag[U])

  def assert[U](expected: U, actual: U)(implicit CT: ClassTag[U])

  def assertTrue(expected: Boolean)

  def assert[U](message: String, expected: U, actual: U)(implicit CT: ClassTag[U])
}

