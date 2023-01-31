![build status](https://github.com/holdenk/spark-testing-base/actions/workflows/github-actions-basic.yml/badge.svg?branch=master)


# spark-testing-base

Base classes to use when writing tests with Spark.

## Why?

You've written an awesome program in Spark and now its time to write some tests. Only you find yourself writing the code to setup and tear down local mode Spark in between each suite and you say to your self:
This is not my beautiful code.

## How?

So you include com.holdenkarau.spark-testing-base [spark_version]_1.4.0 and extend one of the classes and write some simple tests instead.  For example to include this in a project using Spark 3.0.0:

```scala
"com.holdenkarau" %% "spark-testing-base" % "3.0.0_1.4.0" % "test"
```

or

```
<dependency>
	<groupId>com.holdenkarau</groupId>
	<artifactId>spark-testing-base_2.12</artifactId>
	<version>${spark.version}_1.4.0</version>
	<scope>test</scope>
</dependency>
```

How to use it inside your code? have a look at the [wiki](https://github.com/holdenk/spark-testing-base/wiki) page.

The [Maven repositories page for spark-testing-base](https://mvnrepository.com/artifact/com.holdenkarau) lists the releases available.

The Python package of spark-testing-base is available via:
* PyPI: https://pypi.org/project/spark-testing-base/, e.g. `pip install spark-testing-base`
* Conda: https://anaconda.org/conda-forge/spark-testing-base, e.g. `conda install -c conda-forge spark-testing-base`

## Minimum Memory Requirements and OOMs

The default SBT testing java options are too small to support running many of the tests due to the need to launch Spark in local mode. To increase the amount of memory in a build.sbt file you can add:

```scala
fork in Test := true
javaOptions ++= Seq("-Xms8G", "-Xmx8G", "-XX:MaxPermSize=4048M", "-XX:+CMSClassUnloadingEnabled")
```

Note: if your running in JDK17+ PermSize and ClassnloadingEnabled have been removed so it becomes:

```scala
fork in Test := true
javaOptions ++= Seq("-Xms8G", "-Xmx8G"),
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

If your testing Spark SQL CodeGen make sure to set SPARK_TESTING=true

### Codegen tests and Running Spark Testing Base's own tests

If you are testing codegen it's important to have SPARK_TESTING set to yes, as we do in our github actions.

`SPARK_TESTING=yes ./build/sbt clean +compile +test -DsparkVersion=$SPARK_VERSION`

## Where is this from?

Some of this code is a stripped down version of the test suite bases that are in Apache Spark but are not accessible. Other parts are also inspired by sscheck (scalacheck generators for Spark).

Other parts of this are implemented on top of the test suite bases to make your life even easier.

## How do I build this?

This project is built with sbt.

## What are some other options?

While we hope you choose our library, https://github.com/juanrh/sscheck , https://github.com/hammerlab/spark-tests , https://github.com/wdm0006/DummyRDD , and more https://www.google.com/search?q=python+spark+testing+libraries exist as options.

## [Release Notes](RELEASE_NOTES.md)
