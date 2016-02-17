package com.holdenkarau.spark.testing

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.types._
import org.scalacheck.{Arbitrary, Gen}

object DataframeGenerator {

  def genDataFrame(sc: SparkContext, schema: StructType, minPartitions: Int = 1): Arbitrary[DataFrame] = {
    implicit val arbRow: Arbitrary[Row] = getRowGenerator(schema)
    arbitraryDataFrame(sc, schema, minPartitions)
  }

  def getRowGenerator(schema: StructType): Arbitrary[Row] = {
    val generators: List[Gen[Any]] = createGenerators(schema.fields)
    val listGen: Gen[List[Any]] = Gen.sequence[List[Any], Any](generators)
    Arbitrary{ listGen.map(list => Row.fromSeq(list)) }
  }

  private def arbitraryDataFrame(sc: SparkContext, schema: StructType, minPartitions: Int = 1)
                                (implicit a: Arbitrary[Row]): Arbitrary[DataFrame] = {

    val arbitraryRDDs: Arbitrary[RDD[Row]] = RDDGenerator.arbitraryRDD(sc, minPartitions)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    Arbitrary{arbitraryRDDs.arbitrary.map(sqlContext.createDataFrame(_, schema))}
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
