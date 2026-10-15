"""Rows to Arrow: exact types or None (graphio then uses UNWIND). No server needed."""

import datetime

import pyarrow as pa
import pytest

from graphio.kweladb import to_arrow


def test_types():
    rows = [
        {'i': 1, 'f': 1.5, 's': 'a', 'b': True, 'd': datetime.date(2026, 10, 15), 'l': ['x', 'y']},
        {'i': 2, 'f': 2.0, 's': 'b', 'b': False, 'd': datetime.date(2026, 10, 16), 'l': []},
    ]
    table, keep_nulls = to_arrow(rows)
    assert keep_nulls
    assert table.schema.types == [
        pa.int64(), pa.float64(), pa.string(), pa.bool_(), pa.date32(), pa.list_(pa.string())
    ]  # fmt: skip
    assert table.to_pylist() == rows


@pytest.mark.parametrize(
    'values',
    [
        [1, 2.5],  # Arrow would make 1 a float
        [True, 1],
        ['a', 1],
        [[1], [1.5]],
        [[1], 1],
        [datetime.datetime(2026, 10, 15, 12, 0)],
        [{'nested': {'map': 1}}],
        [b'bytes'],
        [2**63],
        [(1, 2)],
    ],
)
def test_inexact_values_fall_back(values):
    assert to_arrow([{'k': v} for v in values]) is None


def test_nulls():
    # Same keys everywhere: a null is an explicit None.
    table, keep_nulls = to_arrow([{'a': 1, 'b': None}, {'a': None, 'b': 'x'}])
    assert keep_nulls and table.to_pylist() == [{'a': 1, 'b': None}, {'a': None, 'b': 'x'}]
    # Keys missing, no None: a null is a missing key.
    table, keep_nulls = to_arrow([{'a': 1}, {'b': 'x'}])
    assert not keep_nulls
    # Both: a null can't mean both.
    assert to_arrow([{'a': 1, 'b': None}, {'a': 2}]) is None
    # A column of nothing but None.
    table, keep_nulls = to_arrow([{'a': None}, {'a': None}])
    assert keep_nulls and table.column('a').null_count == 2


def test_relationship_rows():
    rows = [
        {'start_id': 1, 'end_id': 2, 'properties': {'w': 1, 'note': None}},
        {'start_id': 2, 'end_id': 3, 'properties': {'w': 2, 'note': 'x'}},
    ]
    table, keep_nulls = to_arrow(rows)
    assert keep_nulls and table.to_pylist() == rows
    # Different property keys: missing, not None.
    rows = [
        {'start_id': 1, 'end_id': 2, 'properties': {'w': 1}},
        {'start_id': 2, 'end_id': 3, 'properties': {'note': 'x'}},
    ]
    table, keep_nulls = to_arrow(rows)
    assert not keep_nulls
    # No properties at all.
    table, keep_nulls = to_arrow([{'start_id': 1, 'end_id': 2, 'properties': {}}])
    assert table.schema.field('properties').type == pa.struct([])
    assert table.to_pylist() == [{'start_id': 1, 'end_id': 2, 'properties': {}}]


def test_append_props_scalars_become_lists():
    rows = [{'id': 1, 'src': 'a'}, {'id': 2, 'src': ['b', 'c']}]
    assert to_arrow(rows) is None
    table, _ = to_arrow(rows, lists=['src'])
    assert table.to_pylist() == [{'id': 1, 'src': ['a']}, {'id': 2, 'src': ['b', 'c']}]
    # Inside a relationship's properties.
    rels = [
        {'start_id': 1, 'end_id': 2, 'properties': {'src': 'a'}},
        {'start_id': 1, 'end_id': 2, 'properties': {'src': ['b']}},
    ]
    table, _ = to_arrow(rels, lists=['src'])
    assert [r['properties']['src'] for r in table.to_pylist()] == [['a'], ['b']]
