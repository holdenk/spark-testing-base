package com.holdenkarau.spark.testing

import java.sql.{Date, Timestamp}

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
  def arbitraryDataFrame(sqlContext: SQLContext, schema: StructType, minPartitions: Int = 1): Arbitrary[DataFrame] = {
    arbitraryDataFrameWithCustomFields(sqlContext, schema, minPartitions)()
  }

  /**
   * Creates a DataFrame Generator for the given Schema, and the given custom generators.
   * custom generators should be in the form of (column index, generator function).
   *
   * Note: The given custom generators should match the required schema,
   * for ex. you can't use Int generator for StringType.
   *
   * @param sqlContext     SQL Context.
   * @param schema         The required Schema.
   * @param minPartitions  minimum number of partitions, defaults to 1.
   * @param userGenerators custom user generators in the form of (column index, generator function).
   *                       column index starts from 0 to length - 1
   * @return Arbitrary DataFrames generator of the required schema.
   */
  def arbitraryDataFrameWithCustomFields(sqlContext: SQLContext, schema: StructType, minPartitions: Int = 1)
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
   * Note: Custom generators should match the required schema, for ex. you can't use Int generator for StringType.
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
    val generatorMap = userGenerators.map(generator => (generator.columnName -> generator)).toMap
    (0 until fields.length).toList.map(index =>
      if (generatorMap.contains(fields(index).name)) generatorMap.get(fields(index).name).get.gen
      else getGenerator(fields(index).dataType))
  }

  private def getGenerator(dataType: DataType): Gen[Any] = {
    dataType match {
      case ByteType => Arbitrary.arbitrary[Byte]
      case ShortType => Arbitrary.arbitrary[Short]
      case IntegerType => Arbitrary.arbitrary[Int]
      case LongType => Arbitrary.arbitrary[Long]
      case FloatType => Arbitrary.arbitrary[Float]
      case DoubleType => Arbitrary.arbitrary[Double]
      case StringType => Arbitrary.arbitrary[String]
      case BinaryType => Arbitrary.arbitrary[Array[Byte]]
      case BooleanType => Arbitrary.arbitrary[Boolean]
      case TimestampType => Arbitrary.arbLong.arbitrary.map(new Timestamp(_))
      case DateType => Arbitrary.arbLong.arbitrary.map(new Date(_))
      case arr: ArrayType => {
        val elementGenerator = getGenerator(arr.elementType)
        return Gen.listOf(elementGenerator)
      }
      case map: MapType => {
        val keyGenerator = getGenerator(map.keyType)
        val valueGenerator = getGenerator(map.valueType)
        val keyValueGenerator: Gen[(Any, Any)] = for {
          key <- keyGenerator
          value <- valueGenerator
        } yield (key, value)

        return Gen.mapOf(keyValueGenerator)
      }
      case row: StructType => return getRowGenerator(row)
      case _ => throw new UnsupportedOperationException(s"Type: $dataType not supported")
    }
  }

}

class ColumnGenerator(val columnName: String, generator: => Gen[Any]) extends java.io.Serializable {
  lazy val gen = generator
}