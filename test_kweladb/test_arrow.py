"""Rows to Arrow: exact types or None (graphio then uses UNWIND); Arrow sources. No server needed."""

import datetime
import decimal

import pyarrow as pa
import pytest

from graphio.kweladb import Inexact, arrow_table, batches, to_arrow


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


@pytest.mark.parametrize(
    ('rows', 'reason'),
    [
        ([{'k': 1}, {'k': 2.5}], "key 'k' has int and float values"),
        ([{'k': datetime.datetime(2026, 10, 15, 12, 0)}], "key 'k' has datetime values"),
        ([{'k': [1]}, {'k': ['a']}], "key 'k' has lists with int and str items"),
        ([{'k': [1]}, {'k': 1}], "key 'k' has lists in some rows and single values in others"),
        (
            [{'a': 1, 'b': None}, {'a': 2}],
            'some rows lack keys that others have, and some values are None',
        ),
        (
            [{'p': {'w': 1}}, {'p': 2}],
            "key 'p' holds a map in some rows and other values in others",
        ),
        ([{'p': {'w': 1}}, {'p': {'w': 'x'}}], "key 'p.w' has int and str values"),
        (['not a map'], 'a row is a str, not a map'),
    ],
)
def test_reasons(rows, reason):
    """Why a set goes over UNWIND, as graphio logs it."""
    with pytest.raises(Inexact) as error:
        arrow_table(rows)
    assert str(error.value) == reason


def test_overflow_reason_names_the_key():
    with pytest.raises(Inexact) as error:
        arrow_table([{'k': 2**63}])
    assert str(error.value).startswith("key 'k': ")


def test_batches_stay_under_the_limit_with_uneven_rows():
    """Rows of very different sizes (short ids, long sequences): every batch is measured."""
    table = pa.table(
        {'id': list(range(20_000)), 'seq': ['x' * 10] * 10_000 + ['y' * 5_000] * 10_000}
    )
    limit = 4 * 2**20
    parts = list(batches(table, limit))
    assert max(b.nbytes for b in parts) <= limit
    assert sum(b.num_rows for b in parts) == table.num_rows
    assert pa.Table.from_batches(parts).equals(table)


# ---- Arrow sources (kweladb.create_nodes and the others) ------------------------------------

from graphio import kweladb as transport  # noqa: E402


def _rows(source):
    return [row for batch in transport._source_batches(source) for row in batch.to_pylist()]


def test_sources_of_every_kind():
    table = pa.table({'id': [1, 2, 3], 'name': ['a', 'b', None]})
    expected = table.to_pylist()
    for source in [
        table,
        table.to_batches()[0],
        table.to_reader(),
        iter(table.to_batches(max_chunksize=1)),
        table.to_batches(max_chunksize=2),
    ]:
        assert _rows(source) == expected, type(source)


def test_dictionaries_decoded_and_defaults_filled_in():
    table = pa.table({'kind': pa.array(['x', 'y', 'x']).dictionary_encode(), 'n': [1, 2, 3]})
    batch = next(transport._source_batches(table, defaults={'source': 'arrow', 'n': 0}))
    assert batch.schema.field('kind').type == pa.string()
    assert batch.to_pylist() == [
        {'kind': 'x', 'n': 1, 'source': 'arrow'},
        {'kind': 'y', 'n': 2, 'source': 'arrow'},
        {'kind': 'x', 'n': 3, 'source': 'arrow'},
    ]


@pytest.mark.parametrize(
    'array',
    [
        pa.array([decimal.Decimal('1.5')]),
        pa.array([b'bytes']),
        pa.array([2**63], type=pa.uint64()),
        pa.array([{'a': 1}]),
        pa.array([[[1]]]),
    ],
)
def test_types_neither_transport_carries_fail(array):
    with pytest.raises(TypeError, match="column 'k'"):
        list(transport._source_batches(pa.table({'k': array})))


def test_relationship_rows_from_arrow():
    table = pa.table({'start_id': [1, 2], 'end_id': [3, 4], 'w': [0.5, None]})
    batches = list(transport._relationship_batches(table, ['id'], ['id'], defaults={'src': 'x'}))
    assert [r for b in batches for r in b.to_pylist()] == [
        {'start_id': 1, 'end_id': 3, 'properties': {'w': 0.5, 'src': 'x'}},
        {'start_id': 2, 'end_id': 4, 'properties': {'w': None, 'src': 'x'}},
    ]
    with pytest.raises(ValueError, match='end_id'):
        list(transport._relationship_batches(pa.table({'start_id': [1]}), ['id'], ['id']))
    # Without properties.
    plain = list(
        transport._relationship_batches(pa.table({'start_id': [1], 'end_id': [2]}), ['id'], ['id'])
    )
    assert plain[0].to_pylist() == [{'start_id': 1, 'end_id': 2, 'properties': {}}]


def test_row_batches_split_across_arrow_batches():
    table = pa.table({'i': list(range(25))})
    sizes = [len(b) for b in transport._row_batches(table.to_batches(max_chunksize=7), 10)]
    assert sizes == [10, 10, 5]
