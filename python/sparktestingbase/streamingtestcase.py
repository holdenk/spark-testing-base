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

add_pyspark_path_if_needed()

from .testcase import SparkTestingBaseReuse

import os
import sys
from itertools import chain
import time
import operator
import tempfile
import random
import struct
from functools import reduce

from pyspark.context import SparkConf, SparkContext, RDD
from pyspark.streaming.context import StreamingContext


class StreamingTestCase(SparkTestingBaseReuse):

    """Basic common test case for Spark Streaming tests. Provides a
    Spark Streaming context as well as some helper methods for creating
    streaming input and collecting streaming output.
    Modeled after PySparkStreamingTestCase."""

    timeout = 15  # seconds
    duration = .5

    @classmethod
    def setUpClass(cls):
        super(StreamingTestCase, cls).setUpClass()
        cls.sc.setCheckpointDir("/tmp")

    @classmethod
    def tearDownClass(cls):
        super(StreamingTestCase, cls).tearDownClass()

    @classmethod
    def _sort_result_based_on_key(cls, result):
        return map(lambda x: sorted(x), result)

    def setUp(self):
        self.ssc = StreamingContext(self.sc, self.duration)

    def tearDown(self):
        self.ssc.stop(False)

    def wait_for(self, result, n):
        start_time = time.time()
        while len(result) < n and time.time() - start_time < self.timeout:
            time.sleep(0.01)
        if len(result) < n:
            print("timeout after", self.timeout)

    def _take(self, dstream, n):
        """
        Return the first `n` elements in the stream (will start and stop).
        """
        results = []

        def take(_, rdd):
            if rdd and len(results) < n:
                results.extend(rdd.take(n - len(results)))

        dstream.foreachRDD(take)

        self.ssc.start()
        self.wait_for(results, n)
        return results

    def _collect(self, dstream, n, block=True):
        """
        Collect each RDDs into the returned list.

        :return: list, which will have the collected items.
        """
        result = []

        def get_output(_, rdd):
            if rdd and len(result) < n:
                r = rdd.collect()
                if r:
                    result.append(r)

        dstream.foreachRDD(get_output)

        if not block:
            return result

        self.ssc.start()
        self.wait_for(result, n)
        return result

    def run_func(self, input, func, expected, sort=False, input2=None):
        """
        @param input: dataset for the test. This should be list of lists
        or list of RDDs.
        @param input2: Optional second dataset for the test. If provided your
        func must take two PythonDStreams as input.
        @param func: wrapped function. This function should return
        PythonDStream.
        @param expected: expected output for this testcase.
        Warning: If output is longer than expected this will silently
        discard the additional output. TODO: fail when this happens.
        """
        if not isinstance(input[0], RDD):
            input = [self.sc.parallelize(d, 1) for d in input]
        input_stream = self.ssc.queueStream(input)
        if input2 and not isinstance(input2[0], RDD):
            input2 = [self.sc.parallelize(d, 1) for d in input2]

        # Apply test function to stream.
        if input2:
            input_stream2 = self.ssc.queueStream(input2)
            stream = func(input_stream, input_stream2)
        else:
            stream = func(input_stream)

        result = self._collect(stream, len(expected))
        if sort:
            self._sort_result_based_on_key(result)
            self._sort_result_based_on_key(expected)
        self.assertEqual(expected, result)
