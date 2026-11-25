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

"""Tests for the comparison helpers. These do not need a SparkSession."""

import unittest
from decimal import Decimal

from pyspark.sql import Row

from sparktestingbase import comparison

NAN = float("nan")
INF = float("inf")

# (rtol, atol) pairs: the default (exact) and a loose tolerance.
TOLERANCES = [(0.0, 0.0), (1e-5, 1e-8), (0.5, 10.0)]


class NonFiniteComparisonTest(unittest.TestCase):
    """Mirrors the cases from SPARK-59514 (apache/spark#58790)."""

    UNEQUAL = [(NAN, 1.0), (NAN, INF), (NAN, -INF), (1.0, INF), (1.0, -INF), (INF, -INF)]
    EQUAL = [(NAN, NAN), (INF, INF), (-INF, -INF)]

    def test_unequal_non_finite_pairs(self):
        for rtol, atol in TOLERANCES:
            for a, b in self.UNEQUAL:
                for x, y in [(a, b), (b, a)]:
                    with self.subTest(x=x, y=y, rtol=rtol, atol=atol):
                        self.assertFalse(comparison.compare_vals(x, y, rtol, atol))

    def test_equal_non_finite_pairs(self):
        for rtol, atol in TOLERANCES:
            for x, y in self.EQUAL:
                with self.subTest(x=x, y=y, rtol=rtol, atol=atol):
                    self.assertTrue(comparison.compare_vals(x, y, rtol, atol))

    def test_nested_non_finite(self):
        for wrap in [lambda v: Row(nested=Row(v=v)), lambda v: [v], lambda v: {"key": v},
                     lambda v: Row(arr=[Row(m={"k": v})])]:
            self.assertFalse(comparison.compare_vals(wrap(NAN), wrap(1.0)))
            self.assertFalse(comparison.compare_vals(wrap(1.0), wrap(NAN)))
            self.assertTrue(comparison.compare_vals(wrap(NAN), wrap(NAN)))
            self.assertFalse(comparison.compare_vals(wrap(INF), wrap(-INF), 0.5, 10.0))

    def test_unordered_rows_with_nan(self):
        rows = [Row(a=NAN), Row(a=1.0), Row(a=None)]
        unequal, _, _ = comparison.compare_row_lists(
            rows, list(reversed(rows)), check_row_order=False)
        self.assertEqual(unequal, 0)
        unequal, _, _ = comparison.compare_row_lists(
            rows, [Row(a=2.0), Row(a=1.0), Row(a=None)], check_row_order=False)
        self.assertEqual(unequal, 1)


class ToleranceTest(unittest.TestCase):

    def test_absolute(self):
        self.assertTrue(comparison.compare_vals(1.0, 1.05, atol=0.1))
        self.assertFalse(comparison.compare_vals(1.0, 1.2, atol=0.1))

    def test_relative_to_expected(self):
        self.assertTrue(comparison.compare_vals(1000.0, 1001.0, rtol=0.01))
        self.assertFalse(comparison.compare_vals(1000.0, 1011.0, rtol=0.01))
        self.assertTrue(comparison.compare_vals(1.0, 1.001, rtol=0.01))
        self.assertFalse(comparison.compare_vals(1.0, 1.1, rtol=0.01))

    def test_decimal(self):
        self.assertTrue(comparison.compare_vals(Decimal("1.00"), Decimal("1.01"), atol=0.1))
        self.assertFalse(comparison.compare_vals(Decimal("1.00"), Decimal("1.5"), atol=0.1))
        self.assertFalse(comparison.compare_vals(Decimal("NaN"), Decimal("1")))
        self.assertTrue(comparison.compare_vals(Decimal("NaN"), Decimal("NaN")))

    def test_ints_are_exact(self):
        self.assertFalse(comparison.compare_vals(1, 2, atol=10))


class StructuralTest(unittest.TestCase):

    def test_none(self):
        self.assertTrue(comparison.compare_vals(None, None))
        self.assertFalse(comparison.compare_vals(1.0, None, atol=1.0))
        self.assertFalse(comparison.compare_vals(None, 1.0, atol=1.0))

    def test_row_length(self):
        # SPARK-59513: extra trailing fields must not be ignored.
        self.assertFalse(comparison.compare_vals((1, 2), (1, 2, 3)))
        self.assertFalse(comparison.compare_vals([1.0], [1.0, 2.0], atol=1.0))

    def test_dicts(self):
        self.assertTrue(comparison.compare_vals({"a": 1.0}, {"a": 1.05}, atol=0.1))
        self.assertFalse(comparison.compare_vals({"a": 1.0}, {"b": 1.0}))

    def test_sort_key_is_total(self):
        values = [None, 1, 2.5, NAN, -INF, "a", b"x", Row(a=1), [1, None], {"k": None}]
        sorted(values, key=comparison.sort_key)
        self.assertEqual(
            sorted([NAN, 3.0, None, -INF, 1], key=comparison.sort_key)[:4],
            [None, -INF, 1, 3.0])


class RowListTest(unittest.TestCase):

    def test_merge_walk_reports_only_missing_rows(self):
        expected = [Row(a=i) for i in range(10)]
        actual = [r for r in expected if r.a != 3]
        unequal, total, diffs = comparison.compare_row_lists(
            expected, actual, check_row_order=False)
        self.assertEqual(unequal, 1)
        self.assertEqual(total, 10)
        self.assertEqual(diffs[0][1], Row(a=3))
        self.assertIsNone(diffs[0][2])

    def test_max_errors_and_format(self):
        expected = [Row(a=i, b="x") for i in range(20)]
        actual = [Row(a=i + 100, b="x") for i in range(20)]
        unequal, total, diffs = comparison.compare_row_lists(
            expected, actual, max_errors=3)
        self.assertEqual((unequal, total, len(diffs)), (20, 20, 3))
        message = comparison.format_diff(diffs, unequal, total)
        self.assertIn("20 of 20 rows differ (100.00000 %), showing the first 3", message)
        self.assertIn("!a=0", message)
        self.assertIn("b='x'", message)
        only_diff = comparison.format_diff(diffs, unequal, total, show_only_diff=True)
        self.assertIn("! a: expected=0, actual=100", only_diff)
        self.assertNotIn("b:", only_diff)


if __name__ == "__main__":
    unittest.main()
