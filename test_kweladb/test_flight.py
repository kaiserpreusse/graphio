"""
NodeSet and RelationshipSet over Flight give exactly the graph they give over UNWIND, on the
same KwelaDB: every scenario runs both ways and the whole graphs are compared.
"""

import datetime
import logging

import pytest

from graphio import NodeModel, NodeSet, RelationshipSet
from graphio import kweladb as transport
from test_kweladb.conftest import FLIGHT

DATABASE = 'graphiokwela'


@pytest.fixture
def uploads(monkeypatch):
    """The tables sent over Flight; sets of 50 rows and more go over Flight."""
    monkeypatch.setattr(transport, 'MIN_ROWS', 50)
    sent = []
    upload = transport._upload

    def counting(uri, database, name, table):
        sent.append((database, name, getattr(table, 'num_rows', None)))  # a stream has no count
        upload(uri, database, name, table)

    monkeypatch.setattr(transport, '_upload', counting)
    return sent


def graph(driver, database=None):
    """The whole graph, independent of IDs."""
    nodes = driver.execute_query(
        'MATCH (n) RETURN labels(n) AS l, properties(n) AS p', database_=database
    ).records
    rels = driver.execute_query(
        'MATCH (a)-[r]->(b) RETURN properties(a) AS a, type(r) AS t, properties(r) AS p, '
        'properties(b) AS b',
        database_=database,
    ).records
    return sorted(repr(sorted(r.items())) for r in nodes), sorted(
        repr(sorted(r.items())) for r in rels
    )


def both_ways(driver, monkeypatch, load, database=None):
    """Run `load` over UNWIND and over Flight, from an empty database each time."""
    results = []
    for flight in (False, True):
        driver.execute_query('MATCH (n) DETACH DELETE n', database_=database)
        if flight:
            monkeypatch.setenv('GRAPHIO_KWELA_FLIGHT', FLIGHT)
        else:
            monkeypatch.delenv('GRAPHIO_KWELA_FLIGHT', raising=False)
        load()
        results.append(graph(driver, database))
    staged = driver.execute_query(
        'CALL kwela.stagedTables() YIELD name RETURN name', database_=database
    )
    assert staged.records == [], 'staged tables left behind'
    assert results[0][0], 'the load created nothing'
    assert results[0] == results[1]


def genes(n, **extra):
    ns = NodeSet(['Gene'], merge_keys=['gid'], **extra)
    for i in range(n):
        ns.add_node({
            'gid': i % (n // 2 + 1),
            'name': f'gene {i}',
            'score': i / 7,
            'active': i % 2 == 0,
            'since': datetime.date(2020, 1, 1) + datetime.timedelta(days=i),
            'tags': [f't{i % 3}', 'x'],
        })  # fmt: skip
    return ns


def test_nodes_create(driver, monkeypatch, uploads):
    both_ways(driver, monkeypatch, lambda: genes(1000).create(driver))
    assert len(uploads) == 1


def test_nodes_merge(driver, monkeypatch, uploads):
    def load():
        genes(1000).create_index(driver)
        genes(1000).merge(driver)
        # Again, with explicit None values: a merge removes those properties.
        ns = NodeSet(['Gene'], merge_keys=['gid'])
        for i in range(0, 600, 3):
            ns.add_node(
                {'gid': i, 'name': None, 'score': i, 'active': None, 'since': None, 'tags': None}
            )
        ns.merge(driver)
        # Missing keys: a merge keeps those properties.
        ns = NodeSet(['Gene'], merge_keys=['gid'])
        for i in range(1, 600, 3):
            ns.add_node({'gid': i, 'name': 'renamed'} if i % 2 else {'gid': i, 'extra': 1})
        ns.merge(driver)

    both_ways(driver, monkeypatch, load)
    assert len(uploads) == 3


def test_nodes_merge_append_and_preserve(driver, monkeypatch, uploads):
    def load():
        for round in range(3):
            ns = NodeSet(['Gene'], merge_keys=['gid'], append_props=['src'], preserve=['name'])
            for i in range(500):
                # Scalars in some rows, lists in others, None and missing too.
                src = [f's{round}', 'x'] if i % 3 == 0 else (None if i % 7 == 0 else f's{round}')
                ns.add_node({'gid': i % 200, 'name': f'{round}-{i}', 'src': src})
            ns.merge(driver)

    both_ways(driver, monkeypatch, load)
    assert len(uploads) == 3


def test_mixed_nulls_and_inexact_types_use_unwind(driver, monkeypatch, uploads):
    def load():
        ns = NodeSet(['Gene'], merge_keys=['gid'])
        for i in range(0, 100, 2):
            ns.add_node({'gid': i, 'name': None})
            ns.add_node({'gid': i + 1})  # missing and None: no exact Arrow form
        ns.merge(driver)
        ns = NodeSet(['Gene'], merge_keys=['gid'])
        for i in range(100, 200, 2):
            ns.add_node({'gid': i, 'score': 1})
            ns.add_node({'gid': i + 1, 'score': 1.5})  # int and float
        ns.create(driver)

    both_ways(driver, monkeypatch, load)
    assert uploads == []


def relationships(n, **extra):
    rs = RelationshipSet('LINKS', ['Gene'], ['Gene'], ['gid'], ['gid'], **extra)
    for i in range(n):
        rs.add_relationship(
            {'gid': i % 300}, {'gid': i * 7 % 300}, {'w': i, 'note': f'n{i}' if i % 4 else None}
        )
    return rs


def test_relationships_create(driver, monkeypatch, uploads):
    def load():
        genes(600).create(driver)
        relationships(3000).create(driver)
        # Without properties.
        rs = RelationshipSet('PLAIN', ['Gene'], ['Gene'], ['gid'], ['gid'])
        for i in range(100):
            rs.add_relationship({'gid': i}, {'gid': i + 1}, {})
        rs.create(driver)

    both_ways(driver, monkeypatch, load)
    assert len(uploads) == 3


def test_relationships_merge(driver, monkeypatch, uploads):
    def load():
        genes(600).create(driver)
        genes(600).create_index(driver)
        for round in range(2):
            rs = RelationshipSet(
                'LINKS', ['Gene'], ['Gene'], ['gid'], ['gid'], append_props=['src']
            )
            for i in range(2000):
                rs.add_relationship(
                    {'gid': i % 300}, {'gid': i * 7 % 300}, {'w': round, 'src': f'r{round}'}
                )
            rs.merge(driver)

    both_ways(driver, monkeypatch, load)
    assert len(uploads) == 3


def test_ogm(driver, monkeypatch, uploads):
    class Person(NodeModel):
        name: str
        age: int
        _labels = ['Person']
        _merge_keys = ['name']

    def load():
        from graphio import Base

        Base.set_driver(driver)
        try:
            people = Person.dataset()
            for i in range(100):
                people.add({'name': f'p{i}', 'age': i})
            people.merge(driver)
            # One object: below MIN_ROWS, over UNWIND.
            Person(name='single', age=1).merge()
        finally:
            Base.set_driver(None)

    both_ways(driver, monkeypatch, load)
    assert len(uploads) == 1


def test_database_parameter(driver, monkeypatch, uploads):
    driver.execute_query(f'CREATE DATABASE {DATABASE} IF NOT EXISTS WAIT', database_='system')
    try:
        driver.execute_query('MATCH (n) DETACH DELETE n')
        both_ways(
            driver, monkeypatch, lambda: genes(500).merge(driver, database=DATABASE), DATABASE
        )
        assert uploads and all(database == DATABASE for database, _, _ in uploads)
        assert driver.execute_query('MATCH (n) RETURN count(n) AS c').records[0]['c'] == 0
    finally:
        driver.execute_query(f'DROP DATABASE {DATABASE} IF EXISTS WAIT', database_='system')


def test_log_says_how_each_set_was_sent(driver, monkeypatch, uploads, caplog):
    driver.execute_query('MATCH (n) DETACH DELETE n')
    monkeypatch.setenv('GRAPHIO_KWELA_FLIGHT', FLIGHT)
    with caplog.at_level(logging.INFO, logger='graphio.kweladb'):
        genes(200).merge(driver)
        mixed = NodeSet(['Mixed'], merge_keys=['gid'])
        for i in range(100):
            mixed.add_node({'gid': i, 'score': 1 if i % 2 else 1.5})
        mixed.merge(driver)
    lines = [r.getMessage() for r in caplog.records if r.name == 'graphio.kweladb']
    assert len(lines) == 2, lines
    assert lines[0].startswith('Gene (merge): 200 rows over Arrow Flight in ')
    assert lines[1] == "Mixed (merge): 100 rows over UNWIND: key 'score' has float and int values"


def test_other_servers_use_unwind(monkeypatch):
    """Without KwelaDB's agent, graphio doesn't ask for its protocol."""

    class Info:
        agent = 'Neo4j/5.26.0'

    class FakeDriver:
        def get_server_info(self):
            return Info()

        def execute_query(self, *args, **kwargs):
            raise AssertionError('asked a non-KwelaDB server for kwela.protocol()')

    monkeypatch.setenv('GRAPHIO_KWELA_FLIGHT', FLIGHT)
    assert not transport._speaks_protocol(FakeDriver())


# ---- Arrow sources: the same graph as the set's own rows ------------------------------------


def arrow_ways(driver, monkeypatch, load_dicts, load_arrow):
    """The graph from the set's dicts over UNWIND, from an Arrow source over Flight, and from
    the same source over UNWIND (Flight off): all three must be equal."""
    results = []
    for load, flight in [(load_dicts, False), (load_arrow, True), (load_arrow, False)]:
        driver.execute_query('MATCH (n) DETACH DELETE n')
        if flight:
            monkeypatch.setenv('GRAPHIO_KWELA_FLIGHT', FLIGHT)
        else:
            monkeypatch.delenv('GRAPHIO_KWELA_FLIGHT', raising=False)
        load()
        results.append(graph(driver))
    assert results[0][0], 'the load created nothing'
    assert results[0] == results[1], 'Arrow over Flight differs'
    assert results[0] == results[2], 'Arrow over UNWIND differs'


def gene_rows(n, round=0):
    return [
        {
            'gid': i % (n // 2 + 1),
            'name': None if i % 5 == 0 else f'gene {i}',
            'score': i / 7,
            'since': datetime.date(2020, 1, 1) + datetime.timedelta(days=i),
            'src': [f's{round}'],
        }
        for i in range(n)
    ]


def test_arrow_nodes_merge(driver, monkeypatch, uploads):
    import pyarrow as pa

    def gene_set():
        return NodeSet(['Gene'], merge_keys=['gid'], append_props=['src'], preserve=['since'],
                       default_props={'origin': 'test'})  # fmt: skip

    def load_dicts():
        for round in range(2):
            ns = gene_set()
            ns.add_nodes(gene_rows(400, round))
            ns.merge(driver)

    def load_arrow():
        for round in range(2):
            source = pa.Table.from_pylist(gene_rows(400, round)).to_reader(max_chunksize=150)
            transport.merge_nodes(gene_set(), driver, source)

    arrow_ways(driver, monkeypatch, load_dicts, load_arrow)


def test_arrow_nodes_create_and_relationships(driver, monkeypatch, uploads):
    import pyarrow as pa

    rels = [{'start_gid': i % 50, 'end_gid': i * 7 % 50, 'w': i, 'note': None if i % 3 else 'x'}
            for i in range(600)]  # fmt: skip

    def rel_set():
        return RelationshipSet(
            'LINKS', ['Gene'], ['Gene'], ['gid'], ['gid'], default_props={'by': 't'}
        )

    def load_dicts():
        ns = NodeSet(['Gene'], merge_keys=['gid'])
        ns.add_nodes([{'gid': i, 'name': f'g{i}'} for i in range(50)])
        ns.create(driver)
        rs = rel_set()
        for r in rels:
            rs.add_relationship({'gid': r['start_gid']}, {'gid': r['end_gid']},
                                {'w': r['w'], 'note': r['note']})  # fmt: skip
        rs.create(driver)
        rs.merge(driver)

    def load_arrow():
        nodes = pa.table({'gid': list(range(50)), 'name': [f'g{i}' for i in range(50)]})
        transport.create_nodes(NodeSet(['Gene'], merge_keys=['gid']), driver, nodes)
        transport.create_relationships(rel_set(), driver, pa.Table.from_pylist(rels))
        transport.merge_relationships(rel_set(), driver, pa.Table.from_pylist(rels))

    arrow_ways(driver, monkeypatch, load_dicts, load_arrow)
