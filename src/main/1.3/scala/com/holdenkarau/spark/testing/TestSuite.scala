package com.holdenkarau.spark.testing

import org.scalatest.Suite

import scala.reflect.ClassTag

trait TestSuite extends TestSuiteLike { self: Suite =>

  override def assertEmpty[U](arr: Array[U])(implicit CT: ClassTag[U]) =
    org.scalatest.Assertions.assert(arr.isEmpty)

  override def assert[U](expected: U, actual: U)(implicit CT: ClassTag[U]) =
    org.scalatest.Assertions.assert(expected === actual)

  override def assertTrue(expected: Boolean) =
    org.scalatest.Assertions.assert(expected === true)

  def assert[U](message: String, expected: U, actual: U)(implicit CT: ClassTag[U]) =
    org.scalatest.Assertions.assert(expected === actual, message)

  override def fail(message: String): Unit =
    org.scalatest.Assertions.fail(message)
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

  override def fail(message: String): Unit =
    org.junit.Assert.fail(message)
}

trait TestSuiteLike {
  def assertEmpty[U](arr: Array[U])(implicit CT: ClassTag[U])

  def assert[U](expected: U, actual: U)(implicit CT: ClassTag[U])

  def assertTrue(expected: Boolean)

  def assert[U](message: String, expected: U, actual: U)(implicit CT: ClassTag[U])

  def fail(message: String)
}

