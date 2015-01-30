# spark-testing-base
Base classes to use when writing tests with Spark.
# Why?
You've written an awesome program in Spark and now its time to write some tests. Only you find yourself
writing the code to setup and tear down local mode Spark in between each suite and you say to your self:
This is not my beautiful code.
So you include com.holdenkarau.spark-testing-base 0.0.1 (good for Spark versions 1.1+) and extend one
of the classes and write some simple tests instead.
# Where is this from?
This code is a stripped down version of the test suite bases that are in Apache Spark but are not accessiable.
