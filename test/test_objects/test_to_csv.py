# Note: Integration tests of the CSV functions rely on a somewhat hacky way to copy the CSV files into running
# Docker containers. Mounting volumes is difficult to transfer from local to GitHub Actions (or other CI).
# So far this is the best way to get the files into the import directory of Neo4j running in Docker.
# Downside is that we need docker-py as dependency and have to install Docker in the CI container.

import os
import tarfile

import docker
import pytest

from graphio.objects.nodeset import NodeSet
from graphio.objects.relationshipset import RelationshipSet


@pytest.fixture
def root_dir():
    return pytest.config.rootdir


@pytest.fixture(scope="session")
def reusable_tmp_dir(tmpdir_factory):
    fn = tmpdir_factory.mktemp("data")
    return fn


@pytest.fixture
def small_nodeset() -> NodeSet:
    ns = NodeSet(['Test'], merge_keys=['uuid'])
    for i in range(100):
        ns.add_node({'uuid': i, 'key': 'value'})

    return ns


@pytest.fixture
def nodeset_multiple_labels():
    ns = NodeSet(['Test', 'Foo', 'Bar'], merge_keys=['uuid'])
    for i in range(100):
        ns.add_node({'uuid': i})

    return ns


@pytest.fixture
def nodeset_multiple_labels_multiple_merge_keys():
    ns = NodeSet(['Test', 'Foo', 'Bar'], merge_keys=['uuid', 'other'])
    for i in range(1000):
        ns.add_node({'uuid': i, 'other': i + 10})

    return ns


# workaround to parameterize a test function with a set of fixtures
# for now the use of external packages that extend pytest is avoided (check https://smarie.github.io/python-pytest-cases/ though)
# see https://miguendes.me/how-to-use-fixtures-as-arguments-in-pytestmarkparametrize
NODSET_FIXTURE_NAMES = ['small_nodeset', 'nodeset_multiple_labels', 'nodeset_multiple_labels_multiple_merge_keys']


@pytest.fixture
def different_nodesets(small_nodeset, nodeset_multiple_labels, nodeset_multiple_labels_multiple_merge_keys):
    nodesets = [small_nodeset, nodeset_multiple_labels, nodeset_multiple_labels_multiple_merge_keys]
    return nodesets


def copy_to_all_docker_containers(path, target='/var/lib/neo4j/import'):
    # prepare file
    os.chdir(os.path.dirname(path))
    srcname = os.path.basename(path)
    print("src " + srcname)
    with tarfile.open("example.tar", 'w') as tar:
        try:
            tar.add(srcname)
        finally:
            tar.close()

    client = docker.from_env()

    for this_container in client.containers.list():
        try:
            with open('example.tar', 'rb') as fd:
                this_container.put_archive(path=target, data=fd)
        except Exception as e:
            print(e)


class TestNodeSetToCSV:

    def test_create_csv_file(self, different_nodesets, tmp_path):
        for ns in different_nodesets:

            num_of_props = len(ns.all_properties_in_nodeset())

            csv_file = ns.to_csv(tmp_path)

            with open(csv_file) as f:
                lines = f.readlines()
                # note that csv has header
                assert len(lines) - 1 == len(ns.nodes)

                for l in lines:
                    l = l.strip()
                    assert len(l.split(',')) == num_of_props

    def test_create_csv_query(self, small_nodeset):
        # override uuid which is used in file name
        small_nodeset.uuid = 'peter'
        query = small_nodeset.create_csv_query()
        print(query)
        assert query == """USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///nodeset_Test_uuid_peter.csv' AS line
CREATE (n:Test)
SET n.key = line.key, n.uuid = toInteger(line.uuid)"""

    # note the workaround to parameterize with fixtures
    # see https://miguendes.me/how-to-use-fixtures-as-arguments-in-pytestmarkparametrize
    @pytest.mark.parametrize('ns', NODSET_FIXTURE_NAMES)
    def test_csv_create(self, graph, clear_graph, neo4j_import_dir, ns, request):
        ns = request.getfixturevalue(ns)

        path = ns.to_csv(neo4j_import_dir)
        query = ns.create_csv_query()

        # note: this is a hack to copy files into a running Docker container from Python
        # needed to run the tests without too many changes locally and in GitHub Actions
        copy_to_all_docker_containers(path, '/var/lib/neo4j/import')

        graph.run(query)

        result = graph.run("MATCH (t:Test) RETURN t").data()
        assert len(result) == len(ns.nodes)
        for row in result:
            print(row)
            for k in ns.all_properties_in_nodeset():
                assert row['t'][k] is not None

    def test_merge_csv_query(self, small_nodeset):
        # override uuid
        small_nodeset.uuid = 'peter'
        query = small_nodeset.merge_csv_query()

        assert query == """USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///nodeset_Test_uuid_peter.csv' AS line
MERGE (n:Test { uuid: toInteger(line.uuid) })
SET n.key = line.key, n.uuid = toInteger(line.uuid)"""

    # note the workaround to parameterize with fixtures
    # see https://miguendes.me/how-to-use-fixtures-as-arguments-in-pytestmarkparametrize
    @pytest.mark.parametrize('ns', NODSET_FIXTURE_NAMES)
    def test_csv_merge(self, graph, clear_graph, neo4j_import_dir, ns, request):
        ns = request.getfixturevalue(ns)

        path = ns.to_csv(neo4j_import_dir)

        # note: this is a hack to copy files into a running Docker container from Python
        # needed to run the tests without too many changes locally and in GitHub Actions
        copy_to_all_docker_containers(path, '/var/lib/neo4j/import')

        query = ns.merge_csv_query()

        # run a few times to test that no additional nodes are created
        graph.run(query)
        graph.run(query)
        graph.run(query)

        result = graph.run("MATCH (t:Test) RETURN t").data()
        assert len(result) == len(ns.nodes)
        for row in result:
            for k in ns.all_properties_in_nodeset():
                assert row['t'][k] is not None


class TestRelationshipSetToCSV:

    def test_to_csv(self, tmp_path):
        rs = RelationshipSet('TEST', ['Test', 'Other'], ['Foo', 'SomeLabel'], ['uuid', 'numerical'], ['uuid', 'value'])

        for i in range(10):
            rs.add_relationship(
                {'uuid': i, 'numerical': 1}, {'uuid': i, 'value': 'foo'}, {'value': i, 'other_value': 'peter'}
            )

        # add a few relationships with different props
        for i in range(10, 20):
            rs.add_relationship(
                {'uuid': i, 'numerical': 1}, {'uuid': i, 'value': 'foo'},
                {'second_value': i, 'other_second_value': 'peter'}
            )

        csv_file = rs.to_csv(tmp_path)
        print(csv_file)

        expected_num_of_fields = len(rs.all_property_keys()) + len(rs.fixed_order_start_node_properties) + len(
            rs.fixed_order_end_node_properties)

        with open(csv_file) as f:
            lines = f.readlines()
            assert len(lines) - 1 == len(rs.relationships)

            for l in lines:
                l = l.strip()
                assert len(l.split(',')) == expected_num_of_fields

    def test_create_csv_query(self):
        rs = RelationshipSet('TEST', ['Test', 'Other'], ['Foo', 'SomeLabel'], ['uuid', 'numerical'], ['uuid', 'value'])
        rs.uuid = 'peter'

        for i in range(10):
            rs.add_relationship(
                {'uuid': i, 'numerical': 1}, {'uuid': i, 'value': 'foo'}, {'value': i, 'other_value': 'peter'}
            )

        # add a few relationships with different props
        for i in range(10, 20):
            rs.add_relationship(
                {'uuid': i, 'numerical': 1}, {'uuid': i, 'value': 'foo'},
                {'second_value': i, 'other_second_value': 'peter'}
            )

        query = rs.csv_query('CREATE')
        print(query)
        assert query == """USING PERIODIC COMMIT 1000 
LOAD CSV WITH HEADERS FROM 'file:///relationshipset_Test_Other_TEST_Foo_SomeLabel_peter.csv' AS line 
MATCH (a:Test:Other), (b:Foo:SomeLabel) 
WHERE a.uuid = toInteger(line.a_uuid) AND a.numerical = toInteger(line.a_numerical) AND b.uuid = toInteger(line.b_uuid) AND b.value = line.b_value 
CREATE (a)-[r:TEST]->(b) 
SET r.other_second_value = line.rel_other_second_value, r.other_value = line.rel_other_value, r.second_value = toInteger(line.rel_second_value), r.value = line.rel_value"""

    def test_relationshipset_csv_create(self, graph, clear_graph, neo4j_import_dir):

        # create the nodes required here
        ns1 = NodeSet(['Test', 'Other'], merge_keys=['uuid', 'numerical'])
        ns2 = NodeSet(['Foo', 'SomeLabel'], merge_keys=['uuid', 'value'])

        for i in range(20):
            ns1.add_node({'uuid': i, 'numerical': 1})
            ns2.add_node({'uuid': i, 'value': 'foo'})

        ns1.create_index(graph)
        ns1.create(graph)
        ns2.create_index(graph)
        ns2.create(graph)

        rs = RelationshipSet('TEST', ['Test', 'Other'], ['Foo', 'SomeLabel'], ['uuid', 'numerical'], ['uuid', 'value'])
        rs.uuid = 'peter'
        rs.create_index(graph)

        for i in range(10):
            rs.add_relationship(
                {'uuid': i, 'numerical': 1}, {'uuid': i, 'value': 'foo'}, {'value': i, 'other_value': 'peter'}
            )

        # add a few relationships with different props
        for i in range(10, 20):
            rs.add_relationship(
                {'uuid': i, 'numerical': 1}, {'uuid': i, 'value': 'foo'},
                {'second_value': i, 'other_second_value': 'peter'}
            )

        path = rs.to_csv(neo4j_import_dir)

        # note: this is a hack to copy files into a running Docker container from Python
        # needed to run the tests without too many changes locally and in GitHub Actions
        copy_to_all_docker_containers(path, '/var/lib/neo4j/import')

        query = rs.csv_query('CREATE')

        graph.run(query)

        result = graph.run("MATCH (source:Test:Other)-[r:TEST]->(target:Foo:SomeLabel) RETURN r").data()
        assert len(result) == len(rs.relationships)

    def test_relationshipset_csv_merge(self, graph, clear_graph, neo4j_import_dir):

        # create the nodes required here
        ns1 = NodeSet(['Test', 'Other'], merge_keys=['uuid', 'numerical'])
        ns2 = NodeSet(['Foo', 'SomeLabel'], merge_keys=['uuid', 'value'])

        for i in range(20):
            ns1.add_node({'uuid': i, 'numerical': 1})
            ns2.add_node({'uuid': i, 'value': 'foo'})

        ns1.create_index(graph)
        ns1.create(graph)
        ns2.create_index(graph)
        ns2.create(graph)

        rs = RelationshipSet('TEST', ['Test', 'Other'], ['Foo', 'SomeLabel'], ['uuid', 'numerical'], ['uuid', 'value'])
        rs.uuid = 'peter'
        rs.create_index(graph)

        for i in range(10):
            rs.add_relationship(
                {'uuid': i, 'numerical': 1}, {'uuid': i, 'value': 'foo'}, {'value': i, 'other_value': 'peter'}
            )

        # add a few relationships with different props
        for i in range(10, 20):
            rs.add_relationship(
                {'uuid': i, 'numerical': 1}, {'uuid': i, 'value': 'foo'},
                {'second_value': i, 'other_second_value': 'peter'}
            )

        path = rs.to_csv(neo4j_import_dir)

        # note: this is a hack to copy files into a running Docker container from Python
        # needed to run the tests without too many changes locally and in GitHub Actions
        copy_to_all_docker_containers(path, '/var/lib/neo4j/import')

        query = rs.csv_query('MERGE')

        # run query a few times to check for duplications
        graph.run(query)
        graph.run(query)
        graph.run(query)

        result = graph.run("MATCH (source:Test:Other)-[r:TEST]->(target:Foo:SomeLabel) RETURN r").data()
        assert len(result) == len(rs.relationships)
