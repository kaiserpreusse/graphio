import docker
import pytest
import logging
from py2neo import Graph
from neobolt.exceptions import ServiceUnavailable
from time import sleep

logging.basicConfig()

log = logging.getLogger(__name__)

CONTAINER_NAME = 'neo4j_graphio_test_run'

NEO4J_PASSWORD = 'test'

NEO4J_VERSIONS = [
    {'version': '3.5', 'ports': (7474, 7473, 7687)},
    {'version': '4.0', 'ports': (8474, 8473, 8687)}
]


@pytest.fixture(scope='session', autouse=True)
def run_neo4j():
    log.debug("Run Docker container.")
    client = docker.from_env()

    containers = []

    # run all Neo4j containers
    for v in NEO4J_VERSIONS:
        container = client.containers.run(image='neo4j:{}'.format(v['version']),
                                          ports={'7474/tcp': v['ports'][0], '7473/tcp': v['ports'][1], '7687/tcp': v['ports'][2]},
                                          environment={'NEO4J_AUTH': 'neo4j/{}'.format(NEO4J_PASSWORD)},
                                          name='{}_{}'.format(CONTAINER_NAME, v['version']),
                                          detach=True,
                                          remove=True,
                                          auto_remove=True)

        containers.append(container)

    # check availability for both containers
    connected = False
    max_retries = 20
    retries = 0

    while not connected:
        try:
            # try to connect to both graphs, if it is not available `graph.run()` will
            # throw a ServiceUnavailable error
            for v in NEO4J_VERSIONS:
                # get Graph, bolt connection to localhost is default
                graph = Graph(password=NEO4J_PASSWORD, port=v['ports'][2])
                graph.run("MATCH (n) RETURN n LIMIT 1")
            connected = True

        except ServiceUnavailable:
            retries += 1
            if retries > max_retries:
                break
            sleep(1)

    yield containers

    # stop container
    for c in containers:
        c.stop()


@pytest.fixture(scope='session', params=NEO4J_VERSIONS)
def graph(request):
    yield Graph(password=NEO4J_PASSWORD, port=request.param['ports'][2])


@pytest.fixture
def clear_graph(graph):
    graph.run("MATCH (n) DETACH DELETE n")
