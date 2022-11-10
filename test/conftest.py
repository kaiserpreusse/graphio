import pytest
import logging
import os
import shutil
from py2neo import Graph
from py2neo.wiring import WireError
from py2neo.client import ConnectionUnavailable
from neo4j import GraphDatabase, Driver

from time import sleep

logging.basicConfig()

log = logging.getLogger(__name__)

NEO4J_PASSWORD = 'test'

RUN_ENVIRONMENT = os.getenv('RUN_ENVIRONMENT', None)

if RUN_ENVIRONMENT == 'github_actions':

    NEO4J_VERSIONS = [
        {'host': 'neo4j42', 'version': '4.2', 'ports': (7474, 7473, 7687), 'uri_prefix': 'bolt', 'lib': 'py2neo'},
        {'host': 'neo4j43', 'version': '4.3', 'ports': (7474, 7473, 7687), 'uri_prefix': 'bolt', 'lib': 'py2neo'},
        {'host': 'neo4j43', 'version': '4.4', 'ports': (7474, 7473, 7687), 'uri_prefix': 'bolt', 'lib': 'py2neo'},
        {'host': 'neo4j51', 'version': '5.1', 'ports': (7474, 7473, 7687), 'uri_prefix': 'bolt', 'lib': 'py2neo'},
        {'host': 'neo4j42', 'version': '4.2', 'ports': (7474, 7473, 7687), 'uri_prefix': 'bolt', 'lib': 'neodriver'},
        {'host': 'neo4j43', 'version': '4.3', 'ports': (7474, 7473, 7687), 'uri_prefix': 'bolt', 'lib': 'neodriver'},
        {'host': 'neo4j43', 'version': '4.4', 'ports': (7474, 7473, 7687), 'uri_prefix': 'bolt', 'lib': 'neodriver'},
        {'host': 'neo4j51', 'version': '5.1', 'ports': (7474, 7473, 7687), 'uri_prefix': 'bolt', 'lib': 'neodriver'},
    ]

else:
    NEO4J_VERSIONS = [
        {'host': 'localhost', 'version': '4.2', 'ports': (10474, 10473, 10687), 'uri_prefix': 'bolt', 'lib': 'py2neo'},
        {'host': 'localhost', 'version': '4.3', 'ports': (11474, 11473, 11687), 'uri_prefix': 'bolt', 'lib': 'py2neo'},
        {'host': 'localhost', 'version': '4.4', 'ports': (12474, 12473, 12687), 'uri_prefix': 'bolt', 'lib': 'py2neo'},
        {'host': 'localhost', 'version': '5.1', 'ports': (13474, 13473, 13687), 'uri_prefix': 'bolt', 'lib': 'py2neo'},
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
                graph = Graph(host=v['host'], password=NEO4J_PASSWORD, port=v['ports'][2], scheme='bolt')
                graph.run("MATCH (n) RETURN n LIMIT 1")
            connected = True

        except (ConnectionRefusedError, WireError, ConnectionResetError, ConnectionUnavailable):
            retries += 1
            log.warning(f"Connection unavailable on try {retries}. Try again in 1 second.")
            if retries > max_retries:
                break
            sleep(1)


@pytest.fixture(scope='session', params=NEO4J_VERSIONS)
def graph(request, wait_for_neo4j):
    if request.param['lib'] == 'py2neo':
        yield Graph(host=request.param['host'], password=NEO4J_PASSWORD, port=request.param['ports'][2], scheme='bolt',
                    secure=False)
    elif request.param['lib'] == 'neodriver':
        uri = f"{request.param['uri_prefix']}://{request.param['host']}:{request.param['ports'][2]}"
        yield GraphDatabase.driver(uri, auth=("neo4j", NEO4J_PASSWORD))


@pytest.fixture
def clear_graph(graph):
    if isinstance(graph, Graph):
        graph.run("MATCH (n) DETACH DELETE n")

    elif isinstance(graph, Driver):
        with graph.session() as s:
            s.run("MATCH (n) DETACH DELETE n")


@pytest.fixture
def root_dir(request):
    return request.config.rootdir
