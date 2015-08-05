[![buildstatus](https://travis-ci.org/holdenk/spark-testing-base.svg?branch=master)](https://travis-ci.org/holdenk/spark-testing-base)
# spark-testing-base
Base classes to use when writing tests with Spark.
# Why?
You've written an awesome program in Spark and now its time to write some tests. Only you find yourself
writing the code to setup and tear down local mode Spark in between each suite and you say to your self:
This is not my beautiful code.

So you include com.holdenkarau.spark-testing-base [spark_version]_0.0.5 and extend one
of the classes and write some simple tests instead. For example to include this in a project using Spark 1.3.0:
"com.holdenkarau" % "spark-testing-base" %% "1.3.0_0.0.5"

Note that new versions (0.0.8+) are built against Spark 1.3.0+ for simplicity, but if you need an old version file an issue and I will re-enable cross-builds for older versions.

This package is also cross compiled against scala 2.10.4 and 2.11.6 in the traditional manner.
# Where is this from?
This code is a stripped down version of the test suite bases that are in Apache Spark but are not accessiable.