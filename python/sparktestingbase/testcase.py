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

"""Provides a common test case base for Python Spark tests"""

from .utils import add_pyspark_path, quiet_py4j

import unittest2
from pyspark.context import SparkContext
import os


class SparkTestingBaseTestCase(unittest2.TestCase):

    """Basic common test case for Spark. Provides a Spark context as sc.
    For non local mode testing you can either override sparkMaster
    or set the enviroment property SPARK_MASTER for non-local mode testing."""

    @classmethod
    def getMaster(cls):
        return os.getenv('SPARK_MASTER', "local[4]")

    def setUp(self):
        """Setup a basic Spark context for testing"""
        self.sc = SparkContext(self.getMaster())
        quiet_py4j()

    def tearDown(self):
        """
        Tear down the basic panda spark test case. This stops the running
        context and does a hack to prevent Akka rebinding on the same port.
        """
        self.sc.stop()
        # To avoid Akka rebinding to the same port, since it doesn't unbind
        # immediately on shutdown
        self.sc._jvm.System.clearProperty("spark.driver.port")


class SparkTestingBaseReuse(unittest2.TestCase):

    """Basic common test case for Spark. Provides a Spark context as sc.
    For non local mode testing you can either override sparkMaster
    or set the enviroment property SPARK_MASTER for non-local mode testing."""

    @classmethod
    def getMaster(cls):
        return os.getenv('SPARK_MASTER', "local[4]")

    @classmethod
    def setUpClass(cls):
        """Setup a basic Spark context for testing"""
        class_name = cls.__name__
        cls.sc = SparkContext(cls.getMaster(), appName=class_name)
        quiet_py4j()

    @classmethod
    def tearDownClass(cls):
        """
        Tear down the basic panda spark test case. This stops the running
        context and does a hack to prevent Akka rebinding on the same port.
        """
        print("stopping class")
        cls.sc.stop()
        # To avoid Akka rebinding to the same port, since it doesn't unbind
        # immediately on shutdown
        cls.sc._jvm.System.clearProperty("spark.driver.port")


if __name__ == "__main__":
    unittest2.main()
