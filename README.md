[![buildstatus](https://travis-ci.org/holdenk/spark-testing-base.svg?branch=master)](https://travis-ci.org/holdenk/spark-testing-base)
[![codecov.io](http://codecov.io/github/holdenk/spark-testing-base/coverage.svg?branch=master)](http://codecov.io/github/holdenk/spark-testing-base?branch=master)
# spark-testing-base
Base classes to use when writing tests with Spark.
# Why?
You've written an awesome program in Spark and now its time to write some tests. Only you find yourself
writing the code to setup and tear down local mode Spark in between each suite and you say to your self:
This is not my beautiful code.

# How?

So you include com.holdenkarau.spark-testing-base [spark_version]_0.0.5 and extend one
of the classes and write some simple tests instead. For example to include this in a project using Spark 1.3.0:

    "com.holdenkarau" % "spark-testing-base" %% "1.3.0_0.0.5"

or

        <dependency>
            <groupId>com.holdenkarau</groupId>
            <artifactId>spark-testing-base</artifactId>
            <version>${spark.version}_0.0.5</version>
            <scope>test</scope>
        </dependency>
Note that new versions (0.0.8+) are built against Spark 1.3.0+ for simplicity, but if you need an old version file an issue and I will re-enable cross-builds for older versions.

This package is also cross compiled against scala 2.10.4 and 2.11.6 in the traditional manner.

# Special considerations

Make sure to disable parallel execution

# Where is this from?
Much of this code is a stripped down version of the test suite bases that are in Apache Spark but are not accessiable. Other parts are also inspiried by ssbase (scalacheck generators for Spark).

Other parts of this are implemented on top of the test suite bases to make your life even easier.
