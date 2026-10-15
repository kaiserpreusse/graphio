"""
Tests of the experimental KwelaDB transport (graphio/kweladb.py), separate from graphio's own
tests and CI: `uv run pytest` collects only test/. Run explicitly, against a KwelaDB started
outside graphio (in the KwelaDB repository: `make graphio-test-up`):

    make test-kweladb           # these tests
    make test-kweladb-suite     # graphio's own suite against KwelaDB, bulk loads over Flight

The unit tests in test_arrow.py need no server.
"""

import os

import pytest
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

BOLT = os.getenv('KWELA_TEST_BOLT', 'bolt://localhost:7690')
FLIGHT = os.getenv('KWELA_TEST_FLIGHT', 'grpc://localhost:8817')


@pytest.fixture(scope='session')
def driver():
    driver = GraphDatabase.driver(BOLT, auth=('neo4j', 'test'))
    try:
        driver.verify_connectivity()
    except ServiceUnavailable:
        pytest.fail(
            f'no KwelaDB at {BOLT}: start one in the KwelaDB repository with `make graphio-test-up`'
        )
    yield driver
    driver.close()


@pytest.fixture(autouse=True)
def flight_off(monkeypatch):
    """Tests switch the transport on themselves."""
    monkeypatch.delenv('GRAPHIO_KWELA_FLIGHT', raising=False)
