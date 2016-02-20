package com.holdenkarau.spark.testing

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.types._
import org.scalacheck.{Arbitrary, Gen}

object DataframeGenerator {

  /**
    * Generates a DataFrame for the required Schama
    *
    * @param sc Spark Context
    * @param schema The required Schema
    * @param minPartitions defaults to 1
    *
    * @return
    */
  def genDataFrame(sc: SparkContext, schema: StructType, minPartitions: Int = 1): Arbitrary[DataFrame] = {
    val arbitraryRDDs: Arbitrary[RDD[Row]] = RDDGenerator.arbitraryRDD(sc, minPartitions)(getRowGenerator(schema))

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    Arbitrary{arbitraryRDDs.arbitrary.map(sqlContext.createDataFrame(_, schema))}
  }

  def getRowGenerator(schema: StructType): Gen[Row] = {
    val generators: List[Gen[Any]] = createGenerators(schema.fields)
    val listGen: Gen[List[Any]] = Gen.sequence[List[Any], Any](generators)
    val generator: Gen[Row] = listGen.map(list => Row.fromSeq(list))
    generator
  }

  private def createGenerators(fields: Array[StructField]): List[Gen[Any]] = {
    fields.toList.map(field => field.dataType match {
      case StringType => Arbitrary.arbitrary[String]
      case IntegerType => Arbitrary.arbitrary[Int]
      case FloatType => Arbitrary.arbitrary[Float]
      case LongType => Arbitrary.arbitrary[Long]
      case DoubleType => Arbitrary.arbitrary[Double]
      case BooleanType => Arbitrary.arbitrary[Boolean]
      case TimestampType => Arbitrary.arbitrary[Long]
      case ByteType => Arbitrary.arbitrary[Byte]
      case ShortType => Arbitrary.arbitrary[Short]
      case BinaryType => Arbitrary.arbitrary[Array[Byte]]
    } )
  }

}
