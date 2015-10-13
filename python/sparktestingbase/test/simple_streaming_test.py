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
        return f1.subtract(f2)

    def test_simple_transformation(self):
        input = [["hi"], ["hi holden"], ["bye"]]
        expected = [["hi"], ["hi", "holden"], ["bye"]]
        self.run_func(input, SimpleStreamingTest.tokenize, expected)

    def test_diff_transformation(self):
        input = [["hi"], ["hi holden"], ["bye"]]
        input2 = [["hi"], ["pandas"], ["bye bye"]]
        expected = [[], ["hi holden"], ["bye"]]
        self.run_func(input, SimpleStreamingTest.difference, expected, input2=input2)

    def test_noop_transformation(self):
        input = [["hi"], ["hi holden"], ["bye"]]
        self.run_func(input, SimpleStreamingTest.noop, input)
        self.run_func(input, SimpleStreamingTest.noop, input, sort=True)


if __name__ == "__main__":
    unittest2.main()
