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
from . import comparison

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
        return {"spark.not.real": "42"}

    def setUp(self):
        from pyspark.sql.session import SparkSession
        self.session = SparkSession.builder.config(map=self.getConf()).getOrCreate()
        self.sqlCtx = SQLContext(self.sc, sparkSession=self.session)

    def assertSchemaEqual(self, expected, result, ignoreNullable=False,
                          ignoreColumnOrder=False, ignoreColumnName=False):
        """Assert that two schemas (StructTypes) are equal.

        :param ignoreNullable: ignore nullability, including nested
            containsNull / valueContainsNull.
        :param ignoreColumnOrder: compare top level fields sorted by name.
        :param ignoreColumnName: compare top level fields by position.
        """
        expected_fields = comparison.normalize_schema(
            expected, ignoreNullable, ignoreColumnOrder, ignoreColumnName)
        result_fields = comparison.normalize_schema(
            result, ignoreNullable, ignoreColumnOrder, ignoreColumnName)
        if expected_fields != result_fields:
            self.fail(comparison.describe_schema_differences(
                expected_fields, result_fields))

    @staticmethod
    def _prepareForComparison(df, ignoreColumnOrder, ignoreColumnName,
                              ignoreColumnType):
        from pyspark.sql.functions import col, regexp_replace
        from pyspark.sql.types import DecimalType, DoubleType, FloatType
        quote = comparison.quote
        if ignoreColumnOrder and not ignoreColumnName:
            df = df.select(*[col(quote(c)) for c in sorted(df.columns)])
        if ignoreColumnName:
            df = df.toDF(*[str(i) for i in range(len(df.columns))])
        if ignoreColumnType:
            columns = []
            for field in df.schema.fields:
                as_string = col(quote(field.name)).cast("string")
                if isinstance(field.dataType, (FloatType, DoubleType, DecimalType)):
                    # Drop trailing zeros so 1 (int) and 1.0 (double) are equal.
                    as_string = regexp_replace(as_string, r"\.0+$", "")
                columns.append(as_string.alias(field.name))
            df = df.select(*columns)
        return df

    def assertDataFrameEqual(self, expected, result, tol=0, checkRowOrder=True,
                             rtol=0.0, atol=None, ignoreNullable=False,
                             ignoreColumnOrder=False, ignoreColumnName=False,
                             ignoreColumnType=False, maxErrors=None,
                             showOnlyDiff=False):
        """Assert that two DataFrames contain the same data.

        ``expected`` and ``result`` may be DataFrames or lists of Rows. The
        schemas are only compared when both are DataFrames, and the column
        options only apply to DataFrames.

        :param tol: absolute tolerance for float and Decimal values (alias
            for ``atol``, kept for backwards compatibility).
        :param checkRowOrder: when False rows are compared ignoring order.
        :param rtol: relative tolerance, relative to the expected value. Two
            finite values are equal when
            ``abs(expected - result) <= atol + rtol * abs(expected)``.
            NaN is only equal to NaN, an infinity only to the same infinity.
        :param atol: absolute tolerance, defaults to ``tol``.
        :param ignoreNullable: ignore nullability when comparing schemas.
        :param ignoreColumnOrder: match columns by name instead of position.
        :param ignoreColumnName: match columns by position, ignoring names.
        :param ignoreColumnType: compare the string form of every column.
        :param maxErrors: maximum number of differing rows to report
            (defaults to 10).
        :param showOnlyDiff: only show the differing fields of each row.
        """
        if atol is None:
            atol = tol
        if maxErrors is None:
            maxErrors = 10

        if not isinstance(expected, list):
            expected = self._prepareForComparison(
                expected, ignoreColumnOrder, ignoreColumnName, ignoreColumnType)
        if not isinstance(result, list):
            result = self._prepareForComparison(
                result, ignoreColumnOrder, ignoreColumnName, ignoreColumnType)
        if not isinstance(expected, list) and not isinstance(result, list):
            self.assertSchemaEqual(expected.schema, result.schema,
                                   ignoreNullable=ignoreNullable)

        expected_rows = expected if isinstance(expected, list) else expected.collect()
        result_rows = result if isinstance(result, list) else result.collect()

        if checkRowOrder and len(expected_rows) != len(result_rows):
            self.fail("Length not Equal: expected %d rows, result has %d rows"
                      % (len(expected_rows), len(result_rows)))

        num_unequal, total, diffs = comparison.compare_row_lists(
            expected_rows, result_rows, check_row_order=checkRowOrder,
            rtol=rtol, atol=atol, max_errors=maxErrors)
        if num_unequal:
            self.fail("There are some unequal rows\n" + comparison.format_diff(
                diffs, num_unequal, total, show_only_diff=showOnlyDiff,
                rtol=rtol, atol=atol))
