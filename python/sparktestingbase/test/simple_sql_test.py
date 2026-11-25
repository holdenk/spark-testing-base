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
"""A simple SQL test case"""

from datetime import datetime
from pyspark.sql import Row
from pyspark.sql.types import StructType
from sparktestingbase.sqltestcase import SQLTestCase
import unittest


class SimpleSQLTest(SQLTestCase):
    """A simple test."""

    def test_empty_expected_equal(self):
        allTypes = self.sc.parallelize([])
        df = self.sqlCtx.createDataFrame(allTypes, StructType([]))
        self.assertDataFrameEqual(df, df)

    def test_simple_expected_equal(self):
        allTypes = self.sc.parallelize([Row(
            i=1, s="string", d=1.0, lng=1,
            b=True, list=[1, 2, 3], dict={"s": 0}, row=Row(a=1),
            time=datetime(2014, 8, 1, 14, 1, 5))])
        df = allTypes.toDF()
        self.assertDataFrameEqual(df, df)

    def test_simple_close_equal(self):
        allTypes1 = self.sc.parallelize([Row(
            i=1, s="string", d=1.0, lng=1,
            b=True, list=[1, 2, 3], dict={"s": 0}, row=Row(a=1),
            time=datetime(2014, 8, 1, 14, 1, 5))])
        allTypes2 = self.sc.parallelize([Row(
            i=1, s="string", d=1.001, lng=1,
            b=True, list=[1, 2, 3], dict={"s": 0}, row=Row(a=1),
            time=datetime(2014, 8, 1, 14, 1, 5))])
        self.assertDataFrameEqual(allTypes1.toDF(), allTypes2.toDF(), 0.1)

    @unittest.expectedFailure
    def test_simple_close_unequal(self):
        allTypes1 = self.sc.parallelize([Row(
            i=1, s="string", d=1.0, lng=1,
            b=True, list=[1, 2, 3], dict={"s": 0}, row=Row(a=1),
            time=datetime(2014, 8, 1, 14, 1, 5))])
        allTypes2 = self.sc.parallelize([Row(
            i=1, s="string", d=1.001, lng=1,
            b=True, list=[1, 2, 3], dict={"s": 0}, row=Row(a=1),
            time=datetime(2014, 8, 1, 14, 1, 5))])
        self.assertDataFrameEqual(allTypes1.toDF(), allTypes2.toDF(), 0.0001)

    @unittest.expectedFailure
    def test_very_simple_close_unequal(self):
        allTypes1 = self.sc.parallelize([Row(d=1.0)])
        allTypes2 = self.sc.parallelize([Row(d=1.001)])
        self.assertDataFrameEqual(allTypes1.toDF(), allTypes2.toDF(), 0.0001)

    @unittest.expectedFailure
    def test_dif_schemas_unequal(self):
        allTypes1 = self.sc.parallelize([Row(d=1.0)])
        allTypes2 = self.sc.parallelize([Row(d="1.0")])
        self.assertDataFrameEqual(allTypes1.toDF(), allTypes2.toDF(), 0.0001)

    @unittest.expectedFailure
    def test_empty_dataframe_unequal(self):
        allTypes = self.sc.parallelize([Row(
            i=1, s="string", d=1.001, lng=1,
            b=True, list=[1, 2, 3], dict={"s": 0}, row=Row(a=1),
            time=datetime(2014, 8, 1, 14, 1, 5))])
        empty = self.sc.parallelize([])
        self.assertDataFrameEqual(
            allTypes.toDF(),
            self.sqlCtx.createDataFrame(empty, allTypes.toDF().schema), 0.1)

    def _df(self, rows, schema):
        return self.session.createDataFrame(rows, schema)

    def test_nan_does_not_equal_number(self):
        nan = float("nan")
        df_nan = self._df([(nan,)], "d double")
        df_one = self._df([(1.0,)], "d double")
        for tol in [0, 0.5, 100]:
            with self.assertRaises(AssertionError):
                self.assertDataFrameEqual(df_nan, df_one, tol)
            with self.assertRaises(AssertionError):
                self.assertDataFrameEqual(df_one, df_nan, tol)
        self.assertDataFrameEqual(df_nan, self._df([(nan,)], "d double"))

    def test_infinities(self):
        inf = float("inf")
        pos = self._df([(inf,)], "d double")
        neg = self._df([(-inf,)], "d double")
        one = self._df([(1.0,)], "d double")
        self.assertDataFrameEqual(pos, self._df([(inf,)], "d double"))
        for a, b in [(pos, neg), (one, pos), (pos, one)]:
            with self.assertRaises(AssertionError):
                self.assertDataFrameEqual(a, b, rtol=0.5, atol=10.0)

    def test_nested_tolerance_and_nan(self):
        schema = "a array<double>, s struct<x: double>, m map<string, double>"
        expected = self._df([([1.0, float("nan")], (1.0,), {"k": 1.0})], schema)
        close = self._df([([1.01, float("nan")], (1.01,), {"k": 1.01})], schema)
        self.assertDataFrameEqual(expected, close, 0.1)
        nested_nan = self._df([([1.0, 2.0], (1.0,), {"k": float("nan")})], schema)
        with self.assertRaises(AssertionError):
            self.assertDataFrameEqual(expected, nested_nan, 10.0)

    def test_none_vs_float(self):
        expected = self._df([(1.0,)], "d double")
        result = self._df([(None,)], "d double")
        with self.assertRaises(AssertionError):
            self.assertDataFrameEqual(expected, result, 0.1)

    def test_rtol(self):
        expected = self._df([(1000.0,)], "d double")
        self.assertDataFrameEqual(expected, self._df([(1001.0,)], "d double"), rtol=0.01)
        with self.assertRaises(AssertionError):
            self.assertDataFrameEqual(expected, self._df([(1100.0,)], "d double"), rtol=0.01)

    def test_unordered(self):
        expected = self._df([(1, 1.0), (2, float("nan")), (3, None)], "i int, d double")
        result = self._df([(3, None), (2, float("nan")), (1, 1.0)], "i int, d double")
        with self.assertRaises(AssertionError):
            self.assertDataFrameEqual(expected, result)
        self.assertDataFrameEqual(expected, result, checkRowOrder=False)
        missing = self._df([(3, None), (1, 1.0)], "i int, d double")
        with self.assertRaises(AssertionError) as ctx:
            self.assertDataFrameEqual(expected, missing, checkRowOrder=False)
        self.assertIn("1 of 3 rows differ", str(ctx.exception))

    def test_list_of_rows(self):
        df = self._df([(1, "a")], "i int, s string")
        self.assertDataFrameEqual([Row(i=1, s="a")], df)
        self.assertDataFrameEqual(df, [Row(i=1, s="a")])
        with self.assertRaises(AssertionError):
            self.assertDataFrameEqual([Row(i=2, s="a")], df)

    def test_ignore_nullable(self):
        from pyspark.sql.types import (ArrayType, DoubleType, StructField,
                                       StructType)
        rows = [([1.0],)]
        nullable = self._df(rows, StructType(
            [StructField("a", ArrayType(DoubleType(), True), True)]))
        not_nullable = self._df(rows, StructType(
            [StructField("a", ArrayType(DoubleType(), False), False)]))
        with self.assertRaises(AssertionError) as ctx:
            self.assertDataFrameEqual(nullable, not_nullable)
        self.assertIn("Schemas differ", str(ctx.exception))
        self.assertDataFrameEqual(nullable, not_nullable, ignoreNullable=True)

    def test_ignore_column_order_name_type(self):
        expected = self._df([(1, "a")], "i int, `s.x` string")
        reordered = self._df([("a", 1)], "`s.x` string, i int")
        renamed = self._df([(1, "a")], "j int, t string")
        retyped = self._df([(1.0, "a")], "i double, `s.x` string")
        for result, kwargs in [(reordered, {"ignoreColumnOrder": True}),
                               (renamed, {"ignoreColumnName": True}),
                               (retyped, {"ignoreColumnType": True})]:
            with self.assertRaises(AssertionError):
                self.assertDataFrameEqual(expected, result)
            self.assertDataFrameEqual(expected, result, **kwargs)

    def test_max_errors_and_show_only_diff(self):
        expected = self._df([(i, "same") for i in range(20)], "i int, s string")
        result = self._df([(i + 1, "same") for i in range(20)], "i int, s string")
        with self.assertRaises(AssertionError) as ctx:
            self.assertDataFrameEqual(expected, result, maxErrors=2, showOnlyDiff=True)
        message = str(ctx.exception)
        self.assertIn("20 of 20 rows differ", message)
        self.assertIn("showing the first 2", message)
        self.assertIn("! i: expected=0, actual=1", message)
        self.assertNotIn("same", message)


if __name__ == "__main__":
    unittest.main()
