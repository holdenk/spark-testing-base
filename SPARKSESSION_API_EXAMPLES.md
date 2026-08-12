# SparkSession-Based Generator API Examples

As of version 2.1.3, spark-testing-base provides SparkSession-based overloads for DataFrame and Dataset generators. This is recommended over the deprecated SQLContext-based methods.

## DataFrameGenerator with SparkSession

### Basic Usage

```scala
import com.holdenkarau.spark.testing.DataFrameGenerator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalacheck.Prop.forAll

val spark = SparkSession.builder().getOrCreate()

val schema = StructType(List(
  StructField("name", StringType, nullable = false),
  StructField("age", IntegerType, nullable = false)
))

// Using SparkSession (Recommended)
val dataframeGen = DataFrameGenerator.arbitraryDataFrame(spark, schema, 1)

val property = forAll(dataframeGen.arbitrary) { df =>
  df.schema == schema && df.count() >= 0
}
```

### With Custom Field Generators

```scala
import org.scalacheck.Gen

val nameGenerator = new ColumnGenerator("name", Gen.oneOf("Alice", "Bob", "Charlie"))
val ageGenerator = new ColumnGenerator("age", Gen.choose(18, 65))

val dataframeGen = DataFrameGenerator.arbitraryDataFrameWithCustomFields(
  spark, schema, 1
)(nameGenerator, ageGenerator)
```

## DatasetGenerator with SparkSession

### Basic Dataset Generation

```scala
import com.holdenkarau.spark.testing.DatasetGenerator
import org.scalacheck.Arbitrary

val spark = SparkSession.builder().getOrCreate()
import spark.implicits._

// Using SparkSession (Recommended)
val datasetGen = DatasetGenerator.genDataset[String](spark, 1)(
  Arbitrary.arbitrary[String]
)

val property = forAll(datasetGen) { dataset =>
  dataset.map(_.length).count() == dataset.count()
}
```

### Sized Dataset Generation

```scala
val sizedDatasetGen = DatasetGenerator.genSizedDataset[(Int, String)](spark, 1) { size =>
  Gen.listOfN(size, Arbitrary.arbitrary[Char]).map(l => (size, l.mkString))
}

val property = forAll(sizedDatasetGen) { dataset =>
  val tuples = dataset.collect()
  tuples.forall { case (size, str) => size == str.length }
}
```

### Custom Class Datasets

```scala
case class Person(name: String, age: Int)

val personGen = DatasetGenerator.genDataset[Person](spark, 1) {
  for {
    name <- Arbitrary.arbitrary[String]
    age <- Arbitrary.arbitrary[Int]
  } yield Person(name, age)
}

val property = forAll(personGen) { dataset =>
  dataset.map(_.age).count() == dataset.count()
}
```

## Backward Compatibility

The original SQLContext-based methods are still available and fully supported:

```scala
// Old API (still works, but deprecated)
val sqlContext = spark.sqlContext
val dataframeGen = DataFrameGenerator.arbitraryDataFrame(sqlContext, schema)
val datasetGen = DatasetGenerator.genDataset[String](sqlContext)(Arbitrary.arbitrary[String])
```

## Migration Guide

To migrate from SQLContext to SparkSession in your tests:

1. **Replace SQLContext with SparkSession:**
   ```scala
   // Old
   val sqlContext = SparkSession.builder().getOrCreate().sqlContext
   DataFrameGenerator.arbitraryDataFrame(sqlContext, schema)
   
   // New
   val spark = SparkSession.builder().getOrCreate()
   DataFrameGenerator.arbitraryDataFrame(spark, schema, 1)
   ```

2. **Note the minPartitions parameter:**
   - SparkSession-based methods require an explicit `minPartitions` parameter
   - SQLContext-based methods have a default value of 1
   - This is due to Scala's limitation on default arguments with overloaded methods

3. **Benefits of SparkSession API:**
   - Uses the recommended Spark API (SQLContext is deprecated since Spark 2.0)
   - Better aligned with modern Spark applications
   - Future-proof your tests as SQLContext may be removed in future Spark versions
