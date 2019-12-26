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


@pytest.fixture(scope='session', autouse=True)
def run_neo4j(graph):
    log.debug("Run Docker container.")
    client = docker.from_env()

    existing_containers = client.containers.list(all=True, filters={'name': CONTAINER_NAME})
    if existing_containers:
        container = existing_containers[0]
        container.start()

    else:
        container = client.containers.run(image='neo4j:3.5',
                                          ports={'7474/tcp': 7474, '7473/tcp': 7473, '7687/tcp': 7687},
                                          environment={'NEO4J_AUTH': 'neo4j/{}'.format(NEO4J_PASSWORD)},
                                          name=CONTAINER_NAME,
                                          detach=True)

    connected = False
    max_retries = 20
    retries = 0

    while not connected:
        try:
            graph.run("MATCH (n) RETURN n LIMIT 1")
            connected = True

        except ServiceUnavailable:
            retries += 1
            if retries > max_retries:
                break
            sleep(1)

    yield container

    # stop container
    container.stop()


@pytest.fixture(scope='session')
def graph():
    return Graph(password=NEO4J_PASSWORD)


@pytest.fixture
def clear_graph(graph):
    graph.run("MATCH (n) DETACH DELETE n")
