import docker
import pytest
import logging
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
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
    client = docker.from_env()

    containers = []

    # run all Neo4j containers
    for v in NEO4J_VERSIONS:
        container = client.containers.run(image='neo4j:{}'.format(v['version']),
                                          ports={'7474/tcp': v['ports'][0], '7473/tcp': v['ports'][1],
                                                 '7687/tcp': v['ports'][2]},
                                          environment={'NEO4J_AUTH': 'neo4j/{}'.format(NEO4J_PASSWORD)},
                                          name='{}_{}'.format(CONTAINER_NAME, v['version']),
                                          detach=True,
                                          remove=True,
                                          auto_remove=True)

        containers.append(container)

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
                neo4j_uri = 'bolt://localhost:{0}'.format(v['ports'][2])
                graph = GraphDatabase.driver(neo4j_uri, auth=("neo4j", NEO4J_PASSWORD))

                with graph.session() as s:
                    s.run("MATCH (n) RETURN n LIMIT 1")
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

    neo4j_uri = '{0}://localhost:{1}'.format(request.param['uri_prefix'], request.param['ports'][2])
    log.info("Neo4j URI: {}".format(neo4j_uri))
    driver = GraphDatabase.driver(neo4j_uri, auth=("neo4j", NEO4J_PASSWORD))

    yield driver

    driver.close()


@pytest.fixture
def clear_graph(graph):
    with graph.session() as s:
        s.run("MATCH (n) DETACH DELETE n")

        # remove indexes
        result = s.run("CALL db.indexes()")

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
            # TODO adapt this for Neo4j 4, the DROP INDEX syntax will be deprecated at some point
            for label in labels:
                q = "DROP INDEX ON :{}({})".format(label, ', '.join(properties))
                s.run(q)
