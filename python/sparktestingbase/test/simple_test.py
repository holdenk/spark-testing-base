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

"""Simple test example"""

from sparktestingbase.testcase import SparkTestingBaseTestCase
import unittest2


class SimpleTest(SparkTestingBaseTestCase):
    """A simple test."""

    def test_basic(self):
        """Test a simple collect."""
        input = ["hello world"]
        rdd = self.sc.parallelize(input)
        result = rdd.collect()
        assert result == input

    def test_assertRDDEquals_true(self):
        """Test a simple coleect."""
        input_expected = ["hello world"]
        input_result = ["hello world"]
        rdd_expected = self.sc.parallelize(input_expected)
        rdd_result = self.sc.parallelize(input_result)
        assert self.assertRDDEquals(rdd_expected, rdd_result) is True

    def test_assertRDDEquals_inverse_terms_false(self):
        """Test a simple coleect."""
        input_expected = ["hello world"]
        input_result = ["world hello"]
        rdd_expected = self.sc.parallelize(input_expected)
        rdd_result = self.sc.parallelize(input_result)
        assert self.assertRDDEquals(rdd_expected, rdd_result) is False

    def test_assertRDDEquals_diff_length_false(self):
        """Test a simple collect."""
        input_expected = [1, 2, 3, 4, 5]
        input_result = [1, 2, 3, 4, 5, 6]
        rdd_expected = self.sc.parallelize(input_expected)
        rdd_result = self.sc.parallelize(input_result)
        assert self.assertRDDEquals(rdd_expected, rdd_result) is False

    def test_assertRDDEquals_inverse_list_true(self):
        """Test a simple coleect."""
        input_expected = [5, 4, 3, 2, 1]
        input_result = [1, 2, 3, 4, 5]
        rdd_expected = self.sc.parallelize(input_expected)
        rdd_result = self.sc.parallelize(input_result)
        assert self.assertRDDEquals(rdd_expected, rdd_result) is True

    def test_assertRDDEuqlsWithOrder_true(self):
        input_expected = [1, 2, 3, 4, 5]
        input_result = [1, 2, 3, 4, 5]
        rdd_expected = self.sc.parallelize(input_expected)
        rdd_result = self.sc.parallelize(input_result)
        assert self.assertRDDEqualsWithOrder(rdd_expected, rdd_result) is True

    def test_assertRDDEuqlsWithOrder_wrong_order_false(self):
        input_expected = [5, 4, 3, 2, 1]
        input_result = [1, 2, 3, 4, 5]
        rdd_expected = self.sc.parallelize(input_expected)
        rdd_result = self.sc.parallelize(input_result)
        assert self.assertRDDEqualsWithOrder(rdd_expected, rdd_result) is False


if __name__ == "__main__":
    unittest2.main()
