from __future__ import absolute_import, print_function

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from pyspark import SparkConf
from pyspark.sql import SparkSession

"""Provides a common test case base for Python Spark tests"""

from .utils import quiet_py4j

import unittest2


class SparkTestingBaseTestCase(unittest2.TestCase):
    """Basic common test case for Spark. Provides a Spark session as spark.
    For non local mode testing you can either override sparkMaster
    set the environment property SPARK_MASTER."""

    @classmethod
    def get_conf(cls):
        conf = SparkConf()
        conf.set('spark.sql.session.timeZone', 'UTC')
        conf.set('spark.sql.shuffle.partitions', '12')
        return conf

    @classmethod
    def create_local_spark_session(cls):
        class_name = cls.__class__.__name__
        spark = SparkSession \
            .builder \
            .master("local[4]") \
            .config(conf=cls.get_conf()) \
            .appName(class_name) \
            .enableHiveSupport() \
            .getOrCreate()
        return spark

    @classmethod
    def create_local_spark_session(cls):
        class_name = cls.__name__
        spark = SparkSession \
            .builder \
            .master("local[4]") \
            .appName(class_name) \
            .config(conf=cls.get_conf()) \
            .enableHiveSupport() \
            .getOrCreate()
        return spark

    @classmethod
    def setUpClass(cls):
        """Setup a basic Spark context for testing"""
        cls.spark = cls.create_local_spark_session()
        quiet_py4j()

    @classmethod
    def tearDownClass(cls):
        """
        Tear down the basic panda spark test case. This stops the running
        context and does a hack to prevent Akka rebinding on the same port.
        """
        cls.spark.stop()
        # To avoid Akka rebinding to the same port, since it doesn't unbind
        # immediately on shutdown
        cls.spark.sparkContext._jvm.System.clearProperty("spark.driver.port")

    @classmethod
    def assert_rdd_equals(cls, expected, result):
        return cls.compare_rdd(expected, result) == []

    @classmethod
    def compare_rdd(cls, expected, result):
        expected_keyed = expected \
            .map(lambda x: (x, 1)) \
            .reduceByKey(lambda x, y: x + y)
        result_keyed = result \
            .map(lambda x: (x, 1)) \
            .reduceByKey(lambda x, y: x + y)
        return expected_keyed.cogroup(result_keyed) \
            .map(lambda x: tuple(map(list, x[1]))) \
            .filter(lambda x: x[0] != x[1]).take(1)

    @classmethod
    def assertRDDEqualsWithOrder(cls, expected, result):
        return cls.compare_rdd_with_order(expected, result) == []

    @classmethod
    def compare_rdd_with_order(cls, expected, result):
        def index_rdd(rdd):
            return rdd.zipWithIndex().map(lambda x: (x[1], x[0]))

        index_expected = index_rdd(expected)
        index_result = index_rdd(result)
        return index_expected.cogroup(index_result) \
            .map(lambda x: tuple(map(list, x[1]))) \
            .filter(lambda x: x[0] != x[1]).take(1)


if __name__ == "__main__":
    unittest2.main()
