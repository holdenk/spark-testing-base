[![buildstatus](https://travis-ci.org/holdenk/spark-testing-base.svg?branch=master)](https://travis-ci.org/holdenk/spark-testing-base)
[![codecov.io](http://codecov.io/github/holdenk/spark-testing-base/coverage.svg?branch=master)](http://codecov.io/github/holdenk/spark-testing-base?branch=master)

# spark-testing-base

Base classes to use when writing tests with Spark.

## Why?

You've written an awesome program in Spark and now its time to write some tests. Only you find yourself writing the code to setup and tear down local mode Spark in between each suite and you say to your self:
This is not my beautiful code.

## How?

So you include com.holdenkarau.spark-testing-base [spark_version]_0.7.0 and extend one of the classes and write some simple tests instead.  For example to include this in a project using Spark 2.1.1:

```scala
"com.holdenkarau" %% "spark-testing-base" % "2.1.1_0.7.0" % "test"
```

or

```
<dependency>
    <groupId>com.holdenkarau</groupId>
    <artifactId>spark-testing-base_2.11</artifactId>
    <version>${spark.version}_0.7.0</version>
    <scope>test</scope>
</dependency>
```

How to use it inside your code? have a look at the [wiki](https://github.com/holdenk/spark-testing-base/wiki) page.

The [Maven repositories page for spark-testing-base](https://mvnrepository.com/artifact/com.holdenkarau) lists the releases available.

## Minimum Memory Requirements and OOMs

The default SBT testing java options are too small to support running many of the tests due to the need to launch Spark in local mode. To increase the amount of memory in a build.sbt file you can add:

```scala
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
```

If using surefire you can add:

```
<argLine>-Xmx2048m -XX:MaxPermSize=2048m</argLine>
```

Note: the specific memory values are examples only (and the values used to run spark-testing-base's own tests).

## Special considerations

Make sure to disable parallel execution.

In sbt you can add:

```scala
parallelExecution in Test := false
```

In surefire make sure that forkCount is set to 1 and reuseForks is true.

## Where is this from?

Much of this code is a stripped down version of the test suite bases that are in Apache Spark but are not accessible. Other parts are also inspired by sscheck (scalacheck generators for Spark).

Other parts of this are implemented on top of the test suite bases to make your life even easier.

## What are some other options?

While we hope you choose our library, https://github.com/juanrh/sscheck , https://github.com/hammerlab/spark-tests , https://github.com/wdm0006/DummyRDD , and more https://www.google.com/search?q=python+spark+testing+libraries exist as options.

## [Release Notes](RELEASE_NOTES.md)
