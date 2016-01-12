package com.holdenkarau.spark.testing

class SampleDatasetTest extends DatasetSuiteBase {

  test("equal empty dataset") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val emptyDS = sc.parallelize(List[Person]()).toDS

    equalDatasets(emptyDS, emptyDS)
  }

  test("dataset equal itself") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    
    val list = List(Person("Holden", 2000, 60.0), Person("Hanafy", 23, 80.0))
    val persons = sc.parallelize(list).toDS

    equalDatasets(persons, persons)
  }
  
  test("unequal same schema, different classes") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val list1 = List(Person("Holden", 2000, 60.0), Person("Hanafy", 23, 80.0))
    val list2 = List(SamePerson("Holden", 2000, 60.0), SamePerson("Hanafy", 23, 80.0))

    val persons1 = sc.parallelize(list1).toDS
    val persons2 = sc.parallelize(list2).toDS

    intercept[org.scalatest.exceptions.TestFailedException] {
      equalDatasets(persons1, persons2)
    }
  }

  test("unequal different strings") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val list1 = List(Person("Holden", 2000, 60.0), Person("Hanafy", 23, 80.0))
    val list2 = List(Person("Holden", 2000, 60.0), Person("Hanafi", 23, 80.0))

    val persons1 = sc.parallelize(list1).toDS
    val persons2 = sc.parallelize(list2).toDS

    intercept[org.scalatest.exceptions.TestFailedException] {
      equalDatasets(persons1, persons2)
    }
  }

  test("unequal different integers") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val list1 = List(Person("Holden", 2000, 60.0), Person("Hanafy", 23, 80.0))
    val list2 = List(Person("Holden", 2001, 60.0), Person("Hanafy", 23, 80.0))

    val persons1 = sc.parallelize(list1).toDS
    val persons2 = sc.parallelize(list2).toDS

    intercept[org.scalatest.exceptions.TestFailedException] {
      equalDatasets(persons1, persons2)
    }
  }

  test("unequal different doubles") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val list1 = List(Person("Holden", 2000, 60.0), Person("Hanafy", 23, 80.0))
    val list2 = List(Person("Holden", 2000, 60.0), Person("Hanafy", 23, 80.01))

    val persons1 = sc.parallelize(list1).toDS
    val persons2 = sc.parallelize(list2).toDS

    intercept[org.scalatest.exceptions.TestFailedException] {
      equalDatasets(persons1, persons2)
    }
  }

  test("equals with custom equals") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val list1 = List(CustomPerson("HoLdEn", 2000, 60.0), CustomPerson("HaNaFy", 23, 80.0))
    val list2 = List(CustomPerson("hOlDeN", 2000, 60.0), CustomPerson("hAnAfY", 23, 80.0))

    val persons1 = sc.parallelize(list1).toDS
    val persons2 = sc.parallelize(list2).toDS

    equalDatasets(persons1, persons2)
  }

  test("unequal with custom equals") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val list1 = List(CustomPerson("HoLdEN", 2000, 60.0), CustomPerson("HaNaFy", 23, 80.1))
    val list2 = List(CustomPerson("hOlDeN", 2000, 60.0), CustomPerson("hAnAfY", 23, 80.0))

    val persons1 = sc.parallelize(list1).toDS
    val persons2 = sc.parallelize(list2).toDS

    intercept[org.scalatest.exceptions.TestFailedException] {
      equalDatasets(persons1, persons2)
    }
  }

}

case class Person(name: String, age: Int, weight: Double)

case class SamePerson(name: String, age: Int, weight: Double)

case class CustomPerson(name: String, age: Int, weight: Double) {
  override def equals(obj: Any) = obj match {
    case person: CustomPerson => (name.equalsIgnoreCase(person.name) &&
      age.equals(person.age) && weight.equals(person.weight))
    case _ => false
  }
}