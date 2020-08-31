import pytest
import logging
from py2neo import Graph

from time import sleep

logging.basicConfig()

log = logging.getLogger(__name__)

CONTAINER_NAME = 'neo4j_graphio_test_run'

NEO4J_PASSWORD = 'test'

NEO4J_VERSIONS = [
    {'version': '3.5', 'ports': (9474, 9473, 9687), 'uri_prefix': 'bolt'},
    {'version': '4.0', 'ports': (8474, 8473, 8687), 'uri_prefix': 'bolt'}
]


@pytest.fixture(scope='session', autouse=True)
def run_neo4j():
    log.debug("Run Docker container.")

    # check availability for both containers
    connected = False
    max_retries = 120
    retries = 0

    while not connected:
        try:
            # try to connect to both graphs, if it is not available `graph.run()` will
            # throw a ServiceUnavailable error
            for v in NEO4J_VERSIONS:
                # get Graph, bolt connection to localhost is default
                graph = Graph(password=NEO4J_PASSWORD, port=v['ports'][2], scheme='bolt')
                graph.run("MATCH (n) RETURN n LIMIT 1")
            connected = True

        except (ConnectionRefusedError, ConnectionResetError):
            retries += 1
            if retries > max_retries:
                break
            sleep(1)


@pytest.fixture(scope='session', params=NEO4J_VERSIONS)
def graph(request):
    yield Graph(password=NEO4J_PASSWORD, port=request.param['ports'][2])


@pytest.fixture
def clear_graph(graph):
    graph.run("MATCH (n) DETACH DELETE n")

    # remove indexes
    result = list(
        graph.run("CALL db.indexes()")
    )

    for row in result:
        # the result of the db.indexes() procedure is different for Neo4j 3.5 and 4
        # this should also be synced with differences in py2neo versions
        labels = []
        if 'tokenNames' in row:
            labels = row['tokenNames']
        elif 'labelsOrTypes' in row:
            labels = row['labelsOrTypes']

        properties = row['properties']

        # multiple labels possible?
        for label in labels:
            q = "DROP INDEX ON :{}({})".format(label, ', '.join(properties))
