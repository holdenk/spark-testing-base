/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.holdenkarau.spark.testing

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._
import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Prop.forAll
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

class SampleScalaCheckTest extends FunSuite
    with SharedSparkContext with RDDComparisons with Checkers {
  // tag::propertySample[]
  // A trivial property that the map doesn't change the number of elements
  test("map should not change number of elements") {
    val property =
      forAll(RDDGenerator.genRDD[String](sc)(Arbitrary.arbitrary[String])) {
        rdd => rdd.map(_.length).count() == rdd.count()
      }

    check(property)
  }
  // end::propertySample[]
  // A slightly more complex property check using RDDComparisions
  // tag::propertySample2[]
  test("assert that two methods on the RDD have the same results") {
    val property =
      forAll(RDDGenerator.genRDD[String](sc)(Arbitrary.arbitrary[String])) {
        rdd => compareRDD(filterOne(rdd), filterOther(rdd)).isEmpty
      }

    check(property)
  }
  // end::propertySample2[]

  test("test custom RDD generator") {

    val humanGen: Gen[RDD[Human]] =
      RDDGenerator.genRDD[Human](sc) {
        val generator: Gen[Human] = for {
          name <- Arbitrary.arbitrary[String]
          age <- Arbitrary.arbitrary[Int]
        } yield (Human(name, age))

        generator
      }

    val property =
      forAll(humanGen) {
        rdd => rdd.map(_.age).count() == rdd.count()
      }

    check(property)
  }

  test("test RDD sized generator (nodes)") {

    val nodeGenerator = RDDGenerator.genSizedRDD[(Int, MyNode)](sc) { size: Int =>
      def genGraph(sz: Int): Gen[(Int, MyNode)] = for {
          id <- Arbitrary.arbitrary[Int]
          size <- if (sz <= 0) Gen.const(0) else Gen.choose(0, sz)
          nodes <- Gen.listOfN(size, genGraph(sz / 2))
        } yield {
        (size, MyNode(id, nodes.map{ case (_, node ) => node}))
      }
      genGraph(size)
    }

    val property =
    forAll(nodeGenerator) {
      rdd =>
        /* Getting the rdd size, should be the same one used
          in RandomRDDs.randomRDD(sc, randomDataGenerator, size, numPartitions)
        */
        val rddSize = rdd.count

        val filteredNodes = rdd.filter{ case (size, _) => size > rddSize }

        (rdd.map{ case (_, node) => node.key}.count == rddSize) &&
          filteredNodes.isEmpty
    }
    check(property)
  }


  test("test custom RDD sized generator") {

    val humanGen: Gen[RDD[List[Human]]] =
      RDDGenerator.genSizedRDD[List[Human]](sc) { size: Int =>

        val adultsNumber = math.ceil(size * 0.8).toInt
        val kidsNumber = size - adultsNumber

        def genHuman(genAge: Gen[Int]): Gen[Human] = for {
            name <- Arbitrary.arbitrary[String]
            age <- genAge
        } yield Human(name, age)

        for {
            kids <- Gen.listOfN(kidsNumber, genHuman(Gen.choose(0, 18)))
            adults <- Gen.listOfN(adultsNumber, genHuman(Gen.choose(18, 130)))
        } yield {
          kids ++ adults
        }
      }

    val property =
    forAll(humanGen.map(_.flatMap(identity))) {
      rdd =>
        val rddCount = rdd.count
        val adults = rdd.filter(_.age >= 18).collect
        val kids = rdd.filter(_.age < 18).collect
        val adultsNumber = math.ceil(rdd.count * 0.8).toLong
        val kidsNumber: Long = rddCount - adultsNumber

        adults.size + kids.size == rdd.count &&
          !kids.exists(_.age > 18) &&
          !adults.exists(_.age < 18) &&
          kidsNumber * 4 <= adultsNumber
    }

    check(property)
  }

  test("assert rows' types like schema type") {
    val schema = StructType(
      List(StructField("name", StringType, nullable = false), StructField("age", IntegerType, nullable = false)))
    val rowGen: Gen[Row] = DataframeGenerator.getRowGenerator(schema)
    val property =
      forAll(rowGen) {
        row => row.get(0).isInstanceOf[String] && row.get(1).isInstanceOf[Int]
      }

    check(property)
  }

  test("test generating Dataframes") {
    val schema = StructType(
      List(StructField("name", StringType), StructField("age", IntegerType)))
    val sqlContext = new SQLContext(sc)
    val dataframeGen = DataframeGenerator.arbitraryDataFrame(sqlContext, schema)

    val property =
      forAll(dataframeGen.arbitrary) {
        dataframe => dataframe.schema === schema && dataframe.count >= 0
      }

    check(property)
  }

  test("test custom columns generators") {
    val schema = StructType(
      List(StructField("name", StringType), StructField("age", IntegerType)))
    val sqlContext = new SQLContext(sc)
    val ageGenerator = new Column("age", Gen.choose(10, 100))
    val dataframeGen =
      DataframeGenerator.arbitraryDataFrameWithCustomFields(
        sqlContext, schema)(ageGenerator)

    val property =
      forAll(dataframeGen.arbitrary) {
        dataframe =>
        (dataframe.schema === schema &&
          dataframe.filter("age > 100 OR age < 10").count() == 0)
      }

    check(property)
  }

  test("test multiple columns generators") {
    val schema = StructType(
      List(StructField("name", StringType), StructField("age", IntegerType)))
    val sqlContext = new SQLContext(sc)
    // name should be on of Holden or Hanafy
    val nameGenerator = new Column("name", Gen.oneOf("Holden", "Hanafy"))
    val ageGenerator = new Column("age", Gen.choose(10, 100))
    val dataframeGen =
      DataframeGenerator.arbitraryDataFrameWithCustomFields(
        sqlContext, schema)(nameGenerator, ageGenerator)

    val sqlExpr =
      "(name != 'Holden' AND name != 'Hanafy') OR (age > 100 OR age < 10)"
    val property =
      forAll(dataframeGen.arbitrary) {
        dataframe => dataframe.schema === schema &&
          dataframe.filter(sqlExpr).count() == 0
      }

    check(property)
  }

  test("test multi-level column generators") {
    val schema = StructType(List(
      StructField("user", StructType(List(
        StructField("name", StringType),
        StructField("age", IntegerType),
        StructField("address", StructType(List(
          StructField("street", StringType),
          StructField("zip_code", IntegerType)
        )))
      )))
    ))
    val sqlContext = new SQLContext(sc)
    val userGenerator = new ColumnList("user", Seq(
       // name should be on of Holden or Hanafy
      new Column("name", Gen.oneOf("Holden", "Hanafy")),
      new Column("age", Gen.choose(10, 100)),
      new ColumnList("address", Seq(new Column("zip_code", Gen.choose(100, 200))))
    ))
    val dataframeGen =
      DataframeGenerator.arbitraryDataFrameWithCustomFields(
        sqlContext, schema)(userGenerator)

    val sqlExpr = """
                 |(user.name != 'Holden' AND user.name != 'Hanafy') OR
                 |(user.age > 100 OR user.age < 10) OR
                 |(user.address.zip_code > 200 OR user.address.zip_code < 100)""".
      stripMargin
    val property =
      forAll(dataframeGen.arbitrary) {
        dataframe => dataframe.schema === schema &&
        dataframe.filter(sqlExpr).count() == 0
      }

    check(property)
  }

  test("assert invalid ColumnList does not compile") {
    val schema = StructType(List(
      StructField("user", StructType(List(
        StructField("name", StringType),
        StructField("age", IntegerType)
      )))
    ))
    assertTypeError("""
                    |val userGenerator =
                    |  new ColumnList("user",
                    |    Seq("list", "of", "an", "unsupported", "type"))""".
      stripMargin)
  }

  test("generate rdd of specific size") {
    implicit val generatorDrivenConfig =
      PropertyCheckConfig(minSize = 10, maxSize = 20)
    val prop = forAll(RDDGenerator.genRDD[String](sc)(Arbitrary.arbitrary[String])){
      rdd => rdd.count() <= 20
    }
    check(prop)
  }

  test("test Array Type generator"){
    val schema = StructType(List(StructField("name", StringType, true),
      StructField("pandas", ArrayType(StructType(List(
        StructField("id", LongType, true),
        StructField("zip", StringType, true),
        StructField("happy", BooleanType, true),
        StructField("attributes", ArrayType(FloatType), true)))))))

    val sqlContext = new SQLContext(sc)
    val dataframeGen: Arbitrary[DataFrame] =
      DataframeGenerator.arbitraryDataFrame(sqlContext, schema)
    val property =
      forAll(dataframeGen.arbitrary) {
        dataframe => dataframe.schema === schema &&
          dataframe.select("pandas.attributes").rdd.map(_.getSeq(0)).count() >= 0
      }

    check(property)
  }

  test("timestamp and type generation") {
    val schema = StructType(
      List(StructField("timeStamp", TimestampType), StructField("date", DateType)))
    val sqlContext = new SQLContext(sc)
    val dataframeGen = DataframeGenerator.arbitraryDataFrame(sqlContext, schema)

    val property =
      forAll(dataframeGen.arbitrary) {
        dataframe => {
          dataframe.schema === schema && dataframe.count >= 0
        }
      }

    check(property)
  }

  test("map type generation") {
    val schema = StructType(
      List(StructField("map", MapType(LongType, IntegerType, true))))
    val sqlContext = new SQLContext(sc)
    val dataframeGen = DataframeGenerator.arbitraryDataFrame(sqlContext, schema)

    val property =
      forAll(dataframeGen.arbitrary) {
        dataframe => {
          dataframe.schema === schema && dataframe.count >= 0
        }
      }

    check(property)
  }

  val fields = StructField("byteType", ByteType) ::
    StructField("shortType", ShortType) ::
    StructField("intType", IntegerType) ::
    StructField("longType", LongType) ::
    StructField("doubleType", DoubleType) ::
    StructField("stringType", StringType) ::
    StructField("binaryType", BinaryType) ::
    StructField("booleanType", BooleanType) ::
    StructField("timestampType", TimestampType) ::
    StructField("dateType", DateType) ::
    StructField("arrayType", ArrayType(TimestampType)) ::
    StructField("mapType",
      MapType(LongType, TimestampType, valueContainsNull = true)) ::
    StructField("structType",
      StructType(StructField("timestampType", TimestampType) :: Nil)) :: Nil
  test("second dataframe's evaluation has the same values as first") {
    implicit val generatorDrivenConfig =
      PropertyCheckConfig(minSize = 1, maxSize = 1)

    val sqlContext = new SQLContext(sc)
    val dataframeGen =
      DataframeGenerator.arbitraryDataFrame(sqlContext, StructType(fields))

    val property =
      forAll(dataframeGen.arbitrary) {
        dataframe => {
          val firstEvaluation = dataframe.collect()
          val secondEvaluation = dataframe.collect()
          val zipped = firstEvaluation.zip(secondEvaluation)
          zipped.forall {
            case (r1, r2) => DataFrameSuiteBase.approxEquals(r1, r2, 0.0) }
        }
      }

    check(property)
  }
  test("nullable fields contain null values as well") {
    implicit val generatorDrivenConfig =
      PropertyCheckConfig(minSize = 1, maxSize = 1)
    val nullableFields = fields.map(f => f.copy(nullable = true, name = s"${f.name}Nullable"))
    val sqlContext = new SQLContext(sc)
    val allFields = fields ::: nullableFields
    val dataframeGen =
      DataframeGenerator.arbitraryDataFrame(sqlContext, StructType(allFields))

    val property =
      forAll(Gen.resize(100, dataframeGen.arbitrary)) {
        dataframe => {
          allFields.forall { f =>
            val colValues = dataframe.select(f.name).collect().map(_.get(0))
            if (f.nullable)
              colValues.contains(null) ||
                // Unfortunately, dataframeGen.arbitrary sometimes generates DataFrames where all
                // rows have exactly identical values.
                // In that case, even generating many rows doesn't help to get some nulls...
                // To work around this we check if we generated at least some distinct values.
                colValues.distinct.size < 4 ||
                // This is needed for Array-valued fields where .distinct returns all values, even when
                // they're identical.
                colValues.size == colValues.distinct.size
            else
              !colValues.contains(null)
          }
        }
      }

    check(property)
  }

  private def filterOne(rdd: RDD[String]): RDD[Int] = {
    rdd.filter(_.length > 2).map(_.length)
  }

  private def filterOther(rdd: RDD[String]): RDD[Int] = {
    rdd.map(_.length).filter(_ > 2)
  }
}

case class Human(name: String, age: Int)
case class MyNode(key:Int, children: List[MyNode])

