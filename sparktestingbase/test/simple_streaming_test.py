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

"""Simple streaming test"""

from sparktestingbase.streamingtestcase import StreamingTestCase
import unittest2


class SimpleStreamingTest(StreamingTestCase):
    """A simple test."""

    @classmethod
    def tokenize(cls, f):
        return f.flatMap(lambda line: line.split(" "))

    @classmethod
    def noop(cls, f):
        return f.map(lambda x: x)

    @classmethod
    def difference(cls, f1, f2):
        return f1.transformWith(lambda r1, r2: r1.subtract(r2), f2)

    def test_simple_transformation(self):
        input = [["hi"], ["hi holden"], ["bye"]]
        expected = [["hi"], ["hi", "holden"], ["bye"]]
        self.run_func(input, SimpleStreamingTest.tokenize, expected)

    def test_diff_transformation(self):
        input = [["hi", "pandas"], ["hi holden"], ["bye"]]
        input2 = [["hi"], ["pandas"], ["bye bye"]]
        expected = [["pandas"], ["hi holden"], ["bye"]]
        self.run_func(input, SimpleStreamingTest.difference, expected,
                      input2=input2)

    def test_diff_rdd_transformation(self):
        input = [["hi", "pandas"], ["hi holden"], ["bye"]]
        input_rdd = [self.sc.parallelize(d, 1) for d in input]
        input2 = [["hi"], ["pandas"], ["bye bye"]]
        input2_rdd = [self.sc.parallelize(d, 1) for d in input2]
        expected = [["pandas"], ["hi holden"], ["bye"]]
        self.run_func(input_rdd, SimpleStreamingTest.difference, expected,
                      input2=input2_rdd)

    def test_noop_transformation(self):
        input = [["hi"], ["hi holden"], ["bye"]]
        self.run_func(input, SimpleStreamingTest.noop, input)

    def test_noop_rdd_transformation(self):
        input = [["hi"], ["hi holden"], ["bye"]]
        input_rdd = [self.sc.parallelize(d, 1) for d in input]
        self.run_func(input_rdd, SimpleStreamingTest.noop, input)

    def test_noop_transformation_with_sorting(self):
        input = [["hi"], ["hi holden"], ["bye"]]
        self.run_func(input, SimpleStreamingTest.noop, input, sort=True)

    def test_noop_take(self):
        input = [["hi"], ["hi holden"], ["bye"]]
        input = [self.sc.parallelize(d, 1) for d in input]
        input_stream = self.ssc.queueStream(input)
        self.assertEqual(["hi"], self._take(input_stream, 1))


if __name__ == "__main__":
    unittest2.main()
