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

"""Value, row and schema comparison helpers used by SQLTestCase.

The row comparison is adapted from ``pyspark.testing.utils.assertDataFrameEqual``
(Apache Spark), including the non-finite value handling from SPARK-59514 and the
row length check from SPARK-59513. These functions do not need a SparkSession
or a JVM, so they can be tested on their own.

Semantics:

* ``None`` is only equal to ``None``.
* floats: NaN is only equal to NaN, an infinity is only equal to the same
  infinity, finite values are equal when
  ``abs(expected - actual) <= atol + rtol * abs(expected)``.
* ``Decimal`` values use the same tolerance rule.
* Rows (and other tuples), lists and dicts are compared recursively. Rows and
  lists must have the same length, dicts the same keys.
* Everything else is compared with ``==``.
"""

import datetime
import math
from decimal import Decimal

try:  # VariantVal only exists in Spark 4.0+
    from pyspark.sql.types import VariantVal
except ImportError:  # pragma: no cover - depends on the installed pyspark
    VariantVal = None


def compare_floats(expected, actual, rtol=0.0, atol=0.0):
    """Compare two floats, never applying tolerances to NaN or infinities."""
    if math.isnan(expected) or math.isnan(actual):
        return math.isnan(expected) and math.isnan(actual)
    if math.isinf(expected) or math.isinf(actual):
        return expected == actual
    return abs(expected - actual) <= atol + rtol * abs(expected)


def compare_decimals(expected, actual, rtol=0.0, atol=0.0):
    """Compare two Decimals with the same rules as floats."""
    if expected.is_nan() or actual.is_nan():
        return expected.is_nan() and actual.is_nan()
    if expected.is_infinite() or actual.is_infinite():
        return expected == actual
    return abs(expected - actual) <= Decimal(atol) + Decimal(rtol) * abs(expected)


def compare_vals(expected, actual, rtol=0.0, atol=0.0):
    """Recursively compare two values, see the module docstring for the rules."""
    if expected is None or actual is None:
        return expected is None and actual is None
    if isinstance(expected, float) and isinstance(actual, float):
        return compare_floats(expected, actual, rtol, atol)
    if isinstance(expected, Decimal) and isinstance(actual, Decimal):
        return compare_decimals(expected, actual, rtol, atol)
    # Rows are tuples, as are structs nested inside them.
    if isinstance(expected, tuple) and isinstance(actual, tuple):
        return compare_sequences(expected, actual, rtol, atol)
    if isinstance(expected, list) and isinstance(actual, list):
        return compare_sequences(expected, actual, rtol, atol)
    if isinstance(expected, dict) and isinstance(actual, dict):
        return (
            len(expected) == len(actual)
            and expected.keys() == actual.keys()
            and all(compare_vals(expected[k], actual[k], rtol, atol) for k in expected)
        )
    if (
        VariantVal is not None
        and isinstance(expected, VariantVal)
        and isinstance(actual, VariantVal)
    ):
        return compare_vals(expected.toPython(), actual.toPython(), rtol, atol)
    return expected == actual


def compare_sequences(expected, actual, rtol=0.0, atol=0.0):
    return len(expected) == len(actual) and all(
        compare_vals(e, a, rtol, atol) for e, a in zip(expected, actual)
    )


def compare_rows(expected, actual, rtol=0.0, atol=0.0):
    """Compare two rows; either may be None (a missing row)."""
    return compare_vals(expected, actual, rtol, atol)


def sort_key(value):
    """A total ordering key for (nested) row values.

    Unlike sorting by ``str(row)`` this orders numbers numerically, puts None
    first and NaN after every other number, and never raises on mixed types.
    """
    if value is None:
        return (0,)
    if isinstance(value, bool):
        return (1, 0, int(value))
    if isinstance(value, float):
        if math.isnan(value):
            return (1, 1, 0)
        return (1, 0, value)
    if isinstance(value, Decimal):
        if value.is_nan():
            return (1, 1, 0)
        return (1, 0, value)
    if isinstance(value, int):
        return (1, 0, value)
    if isinstance(value, str):
        return (2, value)
    if isinstance(value, (bytes, bytearray)):
        return (3, bytes(value))
    if isinstance(value, datetime.datetime):
        return (4, value.replace(tzinfo=None).isoformat())
    if isinstance(value, datetime.date):
        return (4, value.isoformat())
    if isinstance(value, (tuple, list)):
        return (5, tuple(sort_key(v) for v in value))
    if isinstance(value, dict):
        return (6, tuple(sorted((sort_key(k), sort_key(v)) for k, v in value.items())))
    return (7, type(value).__name__, str(value))


def _render(value):
    if isinstance(value, tuple) and hasattr(value, "__fields__"):
        return str(value)
    return repr(value)


def _field_names(row):
    try:
        return list(row.__fields__)
    except AttributeError:
        return None


def format_diff(diffs, num_unequal, total_rows, show_only_diff=False, rtol=0.0, atol=0.0):
    """Describe differing rows for a failure message.

    :param diffs: list of (index, expected_row_or_None, actual_row_or_None).
    :param num_unequal: total number of differing rows.
    :param total_rows: total number of rows compared.
    """
    percent = (num_unequal * 100.0 / total_rows) if total_rows else 0.0
    header = "%d of %d rows differ (%.5f %%)" % (num_unequal, total_rows, percent)
    if len(diffs) < num_unequal:
        header += ", showing the first %d" % len(diffs)
    lines = [header]
    for index, expected, actual in diffs:
        if expected is None:
            lines.append("Row %d: unexpected in actual: %s" % (index, _render(actual)))
            continue
        if actual is None:
            lines.append("Row %d: missing from actual: %s" % (index, _render(expected)))
            continue
        names = _field_names(expected) or _field_names(actual) or []
        width = max(len(expected), len(actual))
        same = []
        for i in range(width):
            if i < len(expected) and i < len(actual):
                same.append(compare_vals(expected[i], actual[i], rtol, atol))
            else:
                same.append(False)

        def name(i):
            return names[i] if i < len(names) else "_%d" % i

        if show_only_diff:
            lines.append("Row %d:" % index)
            for i in range(width):
                if not same[i]:
                    e = repr(expected[i]) if i < len(expected) else "<missing>"
                    a = repr(actual[i]) if i < len(actual) else "<missing>"
                    lines.append("    ! %s: expected=%s, actual=%s" % (name(i), e, a))
        else:

            def line(row):
                return "[" + ", ".join(
                    ("" if same[i] else "!") + "%s=%r" % (name(i), row[i])
                    for i in range(len(row))
                ) + "]"

            lines.append("Row %d:" % index)
            lines.append("    expected: " + line(expected))
            lines.append("    actual:   " + line(actual))
    return "\n".join(lines)


def compare_row_lists(expected_rows, actual_rows, check_row_order=True,
                      rtol=0.0, atol=0.0, max_errors=10):
    """Compare two lists of rows.

    Returns ``(num_unequal, total_rows, diffs)`` where ``diffs`` holds at most
    ``max_errors`` entries of ``(index, expected, actual)``; a side is None for
    a row that is missing from it.
    """
    expected_rows = list(expected_rows)
    actual_rows = list(actual_rows)
    if not check_row_order:
        expected_rows = sorted(expected_rows, key=sort_key)
        actual_rows = sorted(actual_rows, key=sort_key)

    diffs = []
    num_unequal = 0

    def record(index, e, a):
        if len(diffs) < max_errors:
            diffs.append((index, e, a))

    if check_row_order or len(expected_rows) == len(actual_rows):
        total = max(len(expected_rows), len(actual_rows))
        for i in range(total):
            e = expected_rows[i] if i < len(expected_rows) else None
            a = actual_rows[i] if i < len(actual_rows) else None
            if not compare_rows(e, a, rtol, atol):
                num_unequal += 1
                record(i, e, a)
    else:
        # Sorted inputs of different lengths: walk them as a merge so a single
        # missing row does not shift every following row (as in SPARK-54090).
        i = j = 0
        total = max(len(expected_rows), len(actual_rows))
        while i < len(expected_rows) or j < len(actual_rows):
            e = expected_rows[i] if i < len(expected_rows) else None
            a = actual_rows[j] if j < len(actual_rows) else None
            if e is not None and a is not None and compare_rows(e, a, rtol, atol):
                i += 1
                j += 1
                continue
            num_unequal += 1
            if a is None or (e is not None and sort_key(e) < sort_key(a)):
                record(i, e, None)
                i += 1
            elif e is None or sort_key(e) > sort_key(a):
                record(j, None, a)
                j += 1
            else:
                record(i, e, a)
                i += 1
                j += 1
    return num_unequal, total, diffs


_AUTO_GENERATED_ALIAS = "__autoGeneratedAlias"


def _normalize_type(data_type, ignore_nullable):
    from pyspark.sql.types import ArrayType, MapType, StructType

    if isinstance(data_type, StructType):
        return StructType([_normalize_field(f, ignore_nullable) for f in data_type.fields])
    if isinstance(data_type, ArrayType):
        return ArrayType(
            _normalize_type(data_type.elementType, ignore_nullable),
            ignore_nullable or data_type.containsNull,
        )
    if isinstance(data_type, MapType):
        return MapType(
            _normalize_type(data_type.keyType, ignore_nullable),
            _normalize_type(data_type.valueType, ignore_nullable),
            ignore_nullable or data_type.valueContainsNull,
        )
    return data_type


def _normalize_field(field, ignore_nullable):
    from pyspark.sql.types import StructField

    metadata = dict(field.metadata or {})
    metadata.pop(_AUTO_GENERATED_ALIAS, None)
    return StructField(
        field.name,
        _normalize_type(field.dataType, ignore_nullable),
        ignore_nullable or field.nullable,
        metadata,
    )


def normalize_schema(schema, ignore_nullable=False, ignore_column_order=False,
                     ignore_column_name=False):
    """Return the list of normalized top level fields of a StructType."""
    fields = [_normalize_field(f, ignore_nullable) for f in schema.fields]
    if ignore_column_order and not ignore_column_name:
        fields = sorted(fields, key=lambda f: f.name)
    if ignore_column_name:
        from pyspark.sql.types import StructField

        fields = [
            StructField(str(i), f.dataType, f.nullable, f.metadata)
            for i, f in enumerate(fields)
        ]
    return fields


def describe_schema_differences(expected_fields, actual_fields):
    lines = ["Schemas differ:"]
    for i in range(max(len(expected_fields), len(actual_fields))):
        e = expected_fields[i] if i < len(expected_fields) else None
        a = actual_fields[i] if i < len(actual_fields) else None
        marker = "  " if e == a else "! "
        lines.append(
            "%s%d: expected=%s, actual=%s"
            % (marker, i, e if e is not None else "<missing>",
               a if a is not None else "<missing>")
        )
    return "\n".join(lines)


def quote(name):
    """Quote a column name so names with dots or backticks resolve correctly."""
    return "`" + name.replace("`", "``") + "`"
