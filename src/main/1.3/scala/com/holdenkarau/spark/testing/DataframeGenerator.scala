package com.holdenkarau.spark.testing

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.scalacheck.{Arbitrary, Gen}

object DataframeGenerator {

  /**
   * Creates a DataFrame Generator for the given Schema.
   *
   * @param sqlContext    SQL Context.
   * @param schema        The required Schema.
   * @param minPartitions minimum number of partitions, defaults to 1.
   * @return Arbitrary DataFrames generator of the required schema.
   */
  def genDataFrame(sqlContext: SQLContext, schema: StructType, minPartitions: Int = 1): Arbitrary[DataFrame] = {
    genDataFrameWithCustomFields(sqlContext, schema, minPartitions)()
  }

  /**
   * Creates a DataFrame Generator for the given Schema, and the given custom generators.
   * custom generators should be in the form of (column index, generator function)
   *
   * @param sqlContext     SQL Context.
   * @param schema         The required Schema.
   * @param minPartitions  minimum number of partitions, defaults to 1.
   * @param userGenerators custom user generators in the form of (column index, generator function).
   *                       column index starts from 0 to length - 1
   * @return Arbitrary DataFrames generator of the required schema.
   */
  def genDataFrameWithCustomFields(sqlContext: SQLContext, schema: StructType, minPartitions: Int = 1)
                                  (userGenerators: ColumnGenerator*): Arbitrary[DataFrame] = {

    val arbitraryRDDs = RDDGenerator.genRDD(sqlContext.sparkContext, minPartitions)(getRowGenerator(schema, userGenerators))
    Arbitrary {
      arbitraryRDDs.map(sqlContext.createDataFrame(_, schema))
    }
  }

  /**
   * Creates row generator for the required schema.
   *
   * @param schema the required Row's schema.
   * @return Gen[Row]
   */
  def getRowGenerator(schema: StructType): Gen[Row] = {
    getRowGenerator(schema, List())
  }

  /**
   * Creates row generator for the required schema and with user's custom generators.
   *
   * @param schema           the required Row's schema.
   * @param customGenerators user custom generator, this is useful if the user want to
   *                         Control specific columns generation.
   * @return Gen[Row]
   */
  def getRowGenerator(schema: StructType, customGenerators: Seq[ColumnGenerator]): Gen[Row] = {
    val generators: List[Gen[Any]] = createGenerators(schema.fields, customGenerators)
    val listGen: Gen[List[Any]] = Gen.sequence[List[Any], Any](generators)
    val generator: Gen[Row] = listGen.map(list => Row.fromSeq(list))
    generator
  }

  private def createGenerators(fields: Array[StructField], userGenerators: Seq[ColumnGenerator]): List[Gen[Any]] = {
    val generatorMap: Map[Int, ColumnGenerator] = userGenerators.map(generator => (generator.index -> generator)).toMap
    (0 until fields.length).toList.map(index =>
      if (generatorMap.contains(index)) generatorMap.get(index).get.gen else getGenerator(fields(index)))
  }

  private def getGenerator(fieldType: StructField): Gen[Any] = {
    val dataType = fieldType.dataType
    dataType match {
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
      case _ => throw new UnsupportedOperationException(s"Type: $dataType not supported")
    }
  }

}

class ColumnGenerator(val index: Int, generator: => Gen[Any]) {
  lazy val gen = generator
}