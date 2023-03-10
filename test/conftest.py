import pytest
import logging
import os
import shutil
from neo4j import GraphDatabase, Driver
from neo4j.exceptions import ServiceUnavailable

from time import sleep

logging.basicConfig()

log = logging.getLogger(__name__)

NEO4J_PASSWORD = 'test'

RUN_ENVIRONMENT = os.getenv('RUN_ENVIRONMENT', None)
DRIVER = os.getenv('DRIVER', None)

if RUN_ENVIRONMENT == 'github_actions':
    NEO4J_VERSIONS = [
        {'host': 'neo4j42', 'version': '4.2', 'ports': (7474, 7473, 7687), 'uri_prefix': 'bolt', 'lib': 'neodriver'},
        {'host': 'neo4j43', 'version': '4.3', 'ports': (7474, 7473, 7687), 'uri_prefix': 'bolt', 'lib': 'neodriver'},
        {'host': 'neo4j43', 'version': '4.4', 'ports': (7474, 7473, 7687), 'uri_prefix': 'bolt', 'lib': 'neodriver'},
        {'host': 'neo4j51', 'version': '5.1', 'ports': (7474, 7473, 7687), 'uri_prefix': 'bolt', 'lib': 'neodriver'},
    ]

else:
    NEO4J_VERSIONS = [
        {'host': 'localhost', 'version': '4.2', 'ports': (10474, 10473, 10687), 'uri_prefix': 'bolt', 'lib': 'neodriver'},
        {'host': 'localhost', 'version': '4.3', 'ports': (11474, 11473, 11687), 'uri_prefix': 'bolt', 'lib': 'neodriver'},
        {'host': 'localhost', 'version': '4.4', 'ports': (12474, 12473, 12687), 'uri_prefix': 'bolt', 'lib': 'neodriver'},
        {'host': 'localhost', 'version': '5.1', 'ports': (13474, 13473, 13687), 'uri_prefix': 'bolt', 'lib': 'neodriver'},
    ]


@pytest.fixture(scope='session')
def wait_for_neo4j():
    # check availability for both containers
    connected = False
    max_retries = 240
    retries = 0

    while not connected:
        try:
            # try to connect to both graphs, if it is not available `graph.run()` will
            # throw a ServiceUnavailable error
            for v in NEO4J_VERSIONS:
                # get Graph, bolt connection to localhost is default
                uri = f"{v['uri_prefix']}://{v['host']}:{v['ports'][2]}"
                driver = GraphDatabase.driver(uri, auth=("neo4j", NEO4J_PASSWORD))
                with driver.session() as s:
                    s.run("MATCH (n) RETURN n LIMIT 1")
            connected = True

        except ServiceUnavailable:
            retries += 1
            log.warning(f"Connection unavailable on try {retries}. Try again in 1 second.")
            if retries > max_retries:
                break
            sleep(1)


@pytest.fixture(scope='session', params=NEO4J_VERSIONS)
def graph(request, wait_for_neo4j):
    if request.param['lib'] == 'neodriver':
        uri = f"{request.param['uri_prefix']}://{request.param['host']}:{request.param['ports'][2]}"
        yield GraphDatabase.driver(uri, auth=("neo4j", NEO4J_PASSWORD))


@pytest.fixture
def clear_graph(graph):
    if isinstance(graph, Driver):
        with graph.session() as s:
            s.run("MATCH (n) DETACH DELETE n")


@pytest.fixture
def root_dir(request):
    return request.config.rootdir
