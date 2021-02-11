from __future__ import absolute_import
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from .utils import add_pyspark_path_if_needed, quiet_py4j
from .pathmagic import *
from .testcase import SparkTestingBaseReuse

import os
import sys
from itertools import chain
import operator
import tempfile
import random
from functools import reduce

from pyspark.context import SparkConf, SparkContext, RDD
from pyspark.sql import SQLContext


class SQLTestCase(SparkTestingBaseReuse):

    """Basic common test case for Spark SQL tests. Provides a
    Spark SQL context as well as some helper methods for comparing
    results."""

    @classmethod
    def setUpClass(cls):
        super(SQLTestCase, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        super(SQLTestCase, cls).tearDownClass()

    def getConf(self):
        """Override this to specify any custom configuration."""
        return {}

    def setUp(self):
        try:
            from pyspark.sql import Session
            self.session = Session.Builder.config(self.getConf())
            self.sqlCtx = self.session._wrapped
        except Exception:
            self.sqlCtx = SQLContext(self.sc)

    def assertDataFrameEqual(self, expected, result, tol=0):
        """Assert that two DataFrames contain the same data.
        When comparing inexact fields uses tol.
        """
        self.assertEqual(expected.schema, result.schema)
        try:
            expectedRDD = expected.rdd.cache()
            resultRDD = result.rdd.cache()
            self.assertEqual(expectedRDD.count(), resultRDD.count())

            def zipWithIndex(rdd):
                """Zip with index (idx, data)"""
                return rdd.zipWithIndex().map(lambda x: (x[1], x[0]))

            def equal(x, y):
                if (len(x) != len(y)):
                    return False
                elif (x == y):
                    return True
                else:
                    for idx in range(len(x)):
                        a = x[idx]
                        b = y[idx]
                        if isinstance(a, float):
                            if (abs(a - b) > tol):
                                return False
                        else:
                            if a != b:
                                return False
                return True
            expectedIndexed = zipWithIndex(expectedRDD)
            resultIndexed = zipWithIndex(resultRDD)
            joinedRDD = expectedIndexed.join(resultIndexed)
            unequalRDD = joinedRDD.filter(
                lambda x: not equal(x[1][0], x[1][1]))
            differentRows = unequalRDD.take(10)
            self.assertEqual([], differentRows)
        finally:
            expectedRDD.unpersist()
            resultRDD.unpersist()
