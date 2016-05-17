package com.holdenkarau.spark.testing

import org.scalatest.FunSuite

class SampleDatasetTest extends FunSuite with DatasetSuiteBase {

  test("equal empty dataset") {
    import sqlContext.implicits._

    val emptyDS = sc.parallelize(List[Person]()).toDS
    assertDatasetEquals(emptyDS, emptyDS)
  }

  test("dataset equal itself") {
    import sqlContext.implicits._
    
    val list = List(Person("Holden", 2000, 60.0), Person("Hanafy", 23, 80.0))
    val persons = sc.parallelize(list).toDS

    assertDatasetEquals(persons, persons)
  }
  
  test("unequal different strings") {
    import sqlContext.implicits._

    val list1 = List(Person("Holden", 2000, 60.0), Person("Hanafy", 23, 80.0))
    val list2 = List(Person("Holden", 2000, 60.0), Person("Hanafi", 23, 80.0))

    val persons1 = sc.parallelize(list1).toDS
    val persons2 = sc.parallelize(list2).toDS

    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDatasetEquals(persons1, persons2)
    }
  }

  test("unequal different integers") {
    import sqlContext.implicits._

    val list1 = List(Person("Holden", 2000, 60.0), Person("Hanafy", 23, 80.0))
    val list2 = List(Person("Holden", 2001, 60.0), Person("Hanafy", 23, 80.0))

    val persons1 = sc.parallelize(list1).toDS
    val persons2 = sc.parallelize(list2).toDS

    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDatasetEquals(persons1, persons2)
    }
  }

  test("unequal different doubles") {
    import sqlContext.implicits._

    val list1 = List(Person("Holden", 2000, 60.0), Person("Hanafy", 23, 80.0))
    val list2 = List(Person("Holden", 2000, 60.0), Person("Hanafy", 23, 80.01))

    val persons1 = sc.parallelize(list1).toDS
    val persons2 = sc.parallelize(list2).toDS

    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDatasetEquals(persons1, persons2)
    }
  }

  test("equals with custom equals") {
    import sqlContext.implicits._

    val list1 = List(CustomPerson("HoLdEn", 2000, 60.0), CustomPerson("HaNaFy", 23, 80.0))
    val list2 = List(CustomPerson("hOlDeN", 2000, 60.0), CustomPerson("hAnAfY", 23, 80.0))

    val persons1 = sc.parallelize(list1).toDS
    val persons2 = sc.parallelize(list2).toDS

    assertDatasetEquals(persons1, persons2)
  }

  test("unequal with custom equals") {
    import sqlContext.implicits._

    val list1 = List(CustomPerson("HoLdEN", 2000, 60.0), CustomPerson("HaNaFy", 23, 80.1))
    val list2 = List(CustomPerson("hOlDeN", 2000, 60.0), CustomPerson("hAnAfY", 23, 80.0))

    val persons1 = sc.parallelize(list1).toDS
    val persons2 = sc.parallelize(list2).toDS

    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDatasetEquals(persons1, persons2)
    }
  }

  test("approximate equal empty dataset") {
    import sqlContext.implicits._

    val emptyDS = sc.parallelize(List[Person]()).toDS

    assertDatasetApproximateEquals(emptyDS, emptyDS, 0.1)
  }

  test("approximate equal same dataset") {
    import sqlContext.implicits._

    val list = List(Person("Holden", 2000, 60.0), Person("Hanafy", 23, 80.0))
    val persons = sc.parallelize(list).toDS

    assertDatasetApproximateEquals(persons, persons, 0.0)
  }

  test("approximate equal with acceptable tolerance") {
    import sqlContext.implicits._

    val list1 = List(Person("Holden", 2000, 60.0), Person("Hanafy", 23, 79.8))
    val list2 = List(Person("Holden", 2000, 60.2), Person("Hanafy", 23, 80.0))

    val persons1 = sc.parallelize(list1).toDS
    val persons2 = sc.parallelize(list2).toDS

    assertDatasetApproximateEquals(persons1, persons2, 0.21)
  }

  test("approximate not equal with low tolerance") {
    import sqlContext.implicits._

    val list1 = List(Person("Holden", 2000, 60.0), Person("Hanafy", 23, 79.8))
    val list2 = List(Person("Holden", 2000, 60.3), Person("Hanafi", 23, 80.0))

    val persons1 = sc.parallelize(list1).toDS
    val persons2 = sc.parallelize(list2).toDS

    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDatasetApproximateEquals(persons1, persons2, 0.2)
    }
  }
}

case class Person(name: String, age: Int, weight: Double)

case class CustomPerson(name: String, age: Int, weight: Double) {
  override def equals(obj: Any) = obj match {
    case person: CustomPerson => (name.equalsIgnoreCase(person.name) &&
      age.equals(person.age) && weight.equals(person.weight))
    case _ => false
  }
}