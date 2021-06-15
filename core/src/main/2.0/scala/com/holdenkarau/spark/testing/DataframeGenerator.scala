package com.holdenkarau.spark.testing

import java.math.{BigDecimal => JavaBigDecimal, RoundingMode}
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
  def arbitraryDataFrame(
    sqlContext: SQLContext, schema: StructType, minPartitions: Int = 1):
      Arbitrary[DataFrame] = {
    arbitraryDataFrameWithCustomFields(sqlContext, schema, minPartitions)()
  }

  /**
   * Creates a DataFrame Generator for the given Schema, and the given custom
   * generators.
   * Custom generators should be specified as a list of:
   * (column index, generator function) tuples.
   *
   * Note: The given custom generators should match the required schema,
   * for ex. you can't use Int generator for StringType.
   *
   * Note 2: The ColumnGenerator* accepted as userGenerators has changed.
   * ColumnGenerator is now the base class of the
   * accepted generators, users upgrading to 0.6 need to change their calls
   * to use Column.  Further explanation can be found in the release notes, and
   * in the class descriptions at the bottom of this file.
   *
   * @param sqlContext     SQL Context.
   * @param schema         The required Schema.
   * @param minPartitions  minimum number of partitions, defaults to 1.
   * @param userGenerators custom user generators in the form of:
   *                       (column index, generator function).
   *                       where column index starts from 0 to length - 1
   * @return Arbitrary DataFrames generator of the required schema.
   */
  def arbitraryDataFrameWithCustomFields(
    sqlContext: SQLContext, schema: StructType, minPartitions: Int = 1)
    (userGenerators: ColumnGenerator*): Arbitrary[DataFrame] = {

    val arbitraryRDDs = RDDGenerator.genRDD(
      sqlContext.sparkContext, minPartitions)(
      getRowGenerator(schema, userGenerators))
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
   * Note: Custom generators should match the required schema, for example
   * you can't use Int generator for StringType.
   *
   * @param schema           the required Row's schema.
   * @param customGenerators user custom generator, this is useful if the you want
   *                         to control specific columns generation.
   * @return Gen[Row]
   */
  def getRowGenerator(
    schema: StructType, customGenerators: Seq[ColumnGenerator]): Gen[Row] = {
    val generators: List[Gen[Any]] =
      createGenerators(schema.fields, customGenerators)
    val listGen: Gen[List[Any]] =
      Gen.sequence[List[Any], Any](generators)
    val generator: Gen[Row] =
      listGen.map(list => Row.fromSeq(list))
    generator
  }

  private def createGenerators(
    fields: Array[StructField],
    userGenerators: Seq[ColumnGenerator]):
      List[Gen[Any]] = {
    val generatorMap = userGenerators.map(
      generator => (generator.columnName -> generator)).toMap
    fields.toList.map { field =>
    if (generatorMap.contains(field.name)) {
        generatorMap.get(field.name) match {
          case Some(gen: Column) => gen.gen
          case Some(list: ColumnList) => getGenerator(field.dataType, list.gen, nullable = field.nullable)
        }
      }
      else getGenerator(field.dataType, nullable = field.nullable)
    }
  }

  private def getGenerator(
    dataType: DataType,
    generators: Seq[ColumnGenerator] = Seq(),
    nullable: Boolean = false): Gen[Any] = {
    val nonNullGen = dataType match {
      case ByteType => Arbitrary.arbitrary[Byte]
      case ShortType => Arbitrary.arbitrary[Short]
      case IntegerType => Arbitrary.arbitrary[Int]
      case LongType => Arbitrary.arbitrary[Long]
      case FloatType => Arbitrary.arbitrary[Float]
      case DoubleType => Arbitrary.arbitrary[Double]
      case StringType => Arbitrary.arbitrary[String]
      case BinaryType => Arbitrary.arbitrary[Array[Byte]]
      case BooleanType => Arbitrary.arbitrary[Boolean]
      case TimestampType => Arbitrary.arbitrary[Long].map{
        l => new Timestamp(l/10000)
      }
      case DateType => Arbitrary.arbLong.arbitrary.map{
        l => new Date(l/10000)
      }
      case dec: DecimalType => {
        Arbitrary.arbitrary[BigDecimal]
          .retryUntil(_.precision <= dec.precision)
          .map(_.bigDecimal.setScale(dec.scale, RoundingMode.HALF_UP))
      }
      case arr: ArrayType => {
        val elementGenerator = getGenerator(arr.elementType, nullable = arr.containsNull)
        Gen.listOf(elementGenerator)
      }
      case map: MapType => {
        val keyGenerator = getGenerator(map.keyType)
        val valueGenerator = getGenerator(map.valueType, nullable = map.valueContainsNull)
        val keyValueGenerator: Gen[(Any, Any)] = for {
          key <- keyGenerator
          value <- valueGenerator
        } yield (key, value)

        Gen.mapOf(keyValueGenerator)
      }
      case row: StructType => getRowGenerator(row, generators)
      case MLUserDefinedType(generator) => generator
      case _ => throw new UnsupportedOperationException(
        s"Type: $dataType not supported")
    }
    if (nullable) {
      // Spark 3.1+ has difficulty with arrays that are null.
      dataType match {
	case arr: ArrayType => nonNullGen
	case _ =>
	  Gen.oneOf(nonNullGen, Gen.const(null))
      }
    } else {
      nonNullGen
    }
  }
}

/**
 * Previously ColumnGenerator. Allows the user to specify a generator for a
 * specific column.
 */
class Column(val columnName: String, generator: => Gen[Any])
    extends ColumnGenerator {
  lazy val gen = generator
}

/**
 * ColumnList allows users to specify custom generators for a list of
 * columns inside a StructType column.
 */
class ColumnList(val columnName: String, generators: => Seq[ColumnGenerator])
    extends ColumnGenerator {
  lazy val gen = generators
}

/**
 * ColumnGenerator - prevously Column; it is now the base class for all
 * ColumnGenerators.
 */
abstract class ColumnGenerator extends java.io.Serializable {
  val columnName: String
}
