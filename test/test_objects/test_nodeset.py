import pytest
import os
import json
import docker
import tarfile
from hypothesis import given, strategies as st

from graphio.objects.nodeset import NodeSet


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

    for this_container in client.containers.list():  # ['graphio_test_neo4j_35', 'graphio_test_neo4j_41', 'graphio_test_neo4j_42']:
        # this_container = client.containers.get(c)
        try:
            with open('example.tar', 'rb') as fd:
                this_container.put_archive(path=target, data=fd)
        except Exception as e:
            print(e)


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


def test_node_set_from_dict():
    people = NodeSet(["Person"], merge_keys=["name"])
    people.add_node({"name": "Tom"})
    people.add_node({"name": "Mary"})
    people_dic = people.to_dict()
    people_copy = NodeSet.from_dict(people_dic)
    assert people_copy.to_dict() == people_dic


def test_str():
    ns = NodeSet(['Test', 'Foo'], merge_keys=['uuid'])
    assert str(ns) == "<NodeSet (['Test', 'Foo']; ['uuid'])>"


def test_nodeset_add_unique():
    ns = NodeSet(['Test', 'Foo'], merge_keys=['name'])
    for i in range(10):
        ns.add_unique({'name': 'Peter'})
    assert len(ns.nodes) == 1


def test_nodeset__estimate_type_of_property_values():
    ns = NodeSet(['Test'], merge_keys=['uuid'])
    for i in range(10):
        ns.add_node({'uuid': i, 'key': 'value', 'foo': 20.5, 'changing': 4})
    # change type for one property to make sure that str is set for inconsistent types
    ns.add_node({'uuid': 1, 'key': 'value', 'foo': 20.5, 'changing': 30.4})

    types = ns._estimate_type_of_property_values()
    assert types['uuid'] == int
    assert types['key'] == str
    assert types['foo'] == float
    assert types['changing'] == str


class TestNodeSetInstances:

    @given(labels=st.lists(st.text(), max_size=10),
           merge_keys=st.lists(st.text(), max_size=10),
           data=st.lists(st.dictionaries(keys=st.text(),
                                         values=st.one_of(st.integers(), st.text(), st.booleans(), st.datetimes())))
           )
    def test_create_instance_add_nodes(self, labels, merge_keys, data):
        ns = NodeSet(labels, merge_keys)
        for i in data:
            ns.add_node(i)


class TestDefaultProps:

    def test_default_props(self):
        ns = NodeSet(['Test', 'Foo', 'Bar'], merge_keys=['uuid'], default_props={'user': 'foo'})
        for i in range(100):
            ns.add_node({'uuid': i})

        for n in ns.nodes:
            assert n['user'] == 'foo'

    def test_default_props_overwrite_from_node(self):
        ns = NodeSet(['Test', 'Foo', 'Bar'], merge_keys=['uuid'], default_props={'user': 'foo'})
        for i in range(100):
            ns.add_node({'uuid': i, 'user': 'bar'})

        for n in ns.nodes:
            assert n['user'] == 'bar'


class TestNodeSetCreate:

    def test_nodeset_create_number(self, small_nodeset, graph, clear_graph):
        small_nodeset.create(graph)

        result = list(
            graph.run(
                "MATCH (n:{}) RETURN count(n)".format(':'.join(small_nodeset.labels))
            )
        )
        print(result)
        assert result[0][0] == 100

    def test_nodeset_create_twice_number(self, small_nodeset, graph, clear_graph):
        small_nodeset.create(graph)
        small_nodeset.create(graph)

        result = list(
            graph.run(
                "MATCH (n:{}) RETURN count(n)".format(':'.join(small_nodeset.labels))
            )
        )
        print(result)
        assert result[0][0] == 200

    def test_nodeset_create_properties(self, small_nodeset, graph, clear_graph):
        small_nodeset.create(graph)

        result = list(
            graph.run(
                "MATCH (n:{}) RETURN n".format(':'.join(small_nodeset.labels))
            )
        )

        for row in result:
            node = row[0]
            assert node['key'] == 'value'

    def test_create_nodeset_multiple_labels(self, nodeset_multiple_labels, graph, clear_graph):
        nodeset_multiple_labels.create(graph)

        result = list(
            graph.run(
                "MATCH (n:{}) RETURN count(n)".format(':'.join(nodeset_multiple_labels.labels))
            )
        )

        assert result[0][0] == 100


class TestNodeSetIndex:

    def test_nodeset_create_single_index(self, graph, clear_graph):
        labels = ['TestNode']
        properties = ['some_key']
        ns = NodeSet(labels, merge_keys=properties)

        ns.create_index(graph)

        result = list(
            graph.run("CALL db.indexes()")
        )

        for row in result:
            # the result of the db.indexes() procedure is different for Neo4j 3.5 and 4
            # this should also be synced with differences in py2neo versions
            if 'tokenNames' in row:
                assert row['tokenNames'] == labels and row['properties'] == properties \
                       or row['tokenNames'] == labels and row['properties'] == properties

            elif 'labelsOrTypes' in row:
                assert row['labelsOrTypes'] == labels and row['properties'] == properties \
                       or row['labelsOrTypes'] == labels and row['properties'] == properties

    def test_nodeset_create_composite_index(self, graph, clear_graph):
        labels = ['TestNode']
        properties = ['some_key', 'other_key']
        ns = NodeSet(labels, merge_keys=properties)

        ns.create_index(graph)

        result = list(
            graph.run("CALL db.indexes()")
        )

        for row in result:
            # the result of the db.indexes() procedure is different for Neo4j 3.5 and 4
            # this should also be synced with differences in py2neo versions
            if 'tokenNames' in row:
                assert row['tokenNames'] == labels and row['properties'] == properties \
                       or row['tokenNames'] == labels and row['properties'] == properties

            elif 'labelsOrTypes' in row:
                assert row['labelsOrTypes'] == labels and row['properties'] == properties \
                       or row['labelsOrTypes'] == labels and row['properties'] == properties

    def test_nodeset_recreate_existing_single_index(self, graph, clear_graph):
        """
        The output/error when you try to recreate an existing index is different in Neo4j 3.5 and 4.

        Create an index a few times to make sure this error is handled.
        """
        labels = ['TestNode']
        properties = ['some_key']
        ns = NodeSet(labels, merge_keys=properties)

        ns.create_index(graph)
        ns.create_index(graph)
        ns.create_index(graph)


class TestNodeSetMerge:
    def test_nodeset_merge_preserve(self, graph, clear_graph):
        """
        Merge a nodeset 3 times and check number of nodes.
        """
        ns = NodeSet(['Test'], merge_keys=['uuid'])
        for i in range(100):
            ns.add_node({'uuid': i, 'key': 'value'})

        ns.merge(graph)

        do_not_overwrite_ns = NodeSet(['Test'], merge_keys=['uuid'], preserve=['key'])
        for i in range(100):
            do_not_overwrite_ns.add_node({'uuid': i, 'key': 'other_value'})

        do_not_overwrite_ns.merge(graph)

        assert list(graph.run("MATCH (n:Test) where n.key = 'value' RETURN count(n)"))[0][0] == 100
        assert list(graph.run("MATCH (n:Test) where n.key = 'other_value' RETURN count(n)"))[0][0] == 0

    def test_nodeset_merge_append_props(self, graph, clear_graph):
        """
        Merge a nodeset 3 times and check number of nodes.
        """
        ns = NodeSet(['Test'], merge_keys=['uuid'], append_props=['key'])
        for i in range(100):
            ns.add_node({'uuid': i, 'key': 'value'})

        ns.merge(graph)

        append_ns = NodeSet(['Test'], merge_keys=['uuid'], append_props=['key'])
        for i in range(100):
            append_ns.add_node({'uuid': i, 'key': 'other_value'})

        append_ns.merge(graph)
        assert list(graph.run("MATCH (n:Test) where 'value' in n.key and 'other_value' in n.key RETURN count(n)"))[0][
                   0] == 100

    def test_nodeset_merge_preserve_and_append_props(self, graph, clear_graph):
        """
        Merge a nodeset 3 times and check number of nodes.
        """
        ns = NodeSet(['Test'], merge_keys=['uuid'], append_props=['key'], preserve=['other_key'])
        for i in range(100):
            ns.add_node({'uuid': i, 'key': 'value', 'other_key': 'bar'})

        ns.merge(graph)
        assert list(graph.run("MATCH (n:Test) where 'value' IN n.key RETURN count(n)"))[0][0] == 100
        assert list(graph.run("MATCH (n:Test) where n.other_key = 'bar' RETURN count(n)"))[0][0] == 100

        append_ns = NodeSet(['Test'], merge_keys=['uuid'], append_props=['key'], preserve=['other_key'])
        for i in range(100):
            append_ns.add_node({'uuid': i, 'key': 'other_value', 'other_key': 'foo'})

        append_ns.merge(graph)

        assert list(graph.run("MATCH (n:Test) where 'value' in n.key and 'other_value' in n.key RETURN count(n)"))[0][
                   0] == 100
        assert list(graph.run("MATCH (n:Test) where n.other_key = 'bar' RETURN count(n)"))[0][0] == 100
        assert list(graph.run("MATCH (n:Test) where n.other_key = 'foo' RETURN count(n)"))[0][0] == 0

    def test_nodeset_merge_preserve_keeps_append_props(self, graph, clear_graph):
        """
        Merge a nodeset 3 times and check number of nodes.
        """
        ns = NodeSet(['Test'], merge_keys=['uuid'], append_props=['key'], preserve=['key'])
        for i in range(100):
            ns.add_node({'uuid': i, 'key': 'value'})

        ns.merge(graph)
        assert list(graph.run("MATCH (n:Test) where 'value' IN n.key RETURN count(n)"))[0][0] == 100

        append_ns = NodeSet(['Test'], merge_keys=['uuid'], append_props=['key'], preserve=['key'])
        for i in range(100):
            append_ns.add_node({'uuid': i, 'key': 'other_value'})

        append_ns.merge(graph)

        assert list(graph.run("MATCH (n:Test) where 'value' IN n.key RETURN count(n)"))[0][0] == 100
        assert list(graph.run("MATCH (n:Test) where 'other_value' IN n.key RETURN count(n)"))[0][0] == 0

    def test_nodeset_merge_number(self, small_nodeset, graph, clear_graph):
        """
        Merge a nodeset 3 times and check number of nodes.
        """
        small_nodeset.merge(graph)
        small_nodeset.merge(graph)
        small_nodeset.merge(graph)

        result = list(
            graph.run("MATCH (n:{}) RETURN count(n)".format(':'.join(small_nodeset.labels)))
        )

        print(result)
        assert result[0][0] == 100


class TestNodeSetSerialize:

    def test_nodeset_file_name(self, small_nodeset):
        # set fixed uuid for small nodeset
        uuid = 'f8d1f0af-3eee-48b4-8407-8694ca628fc0'
        small_nodeset.uuid = uuid
        assert small_nodeset.object_file_name() == f"nodeset_Test_uuid_f8d1f0af-3eee-48b4-8407-8694ca628fc0"
        assert small_nodeset.object_file_name(
            suffix='.json') == "nodeset_Test_uuid_f8d1f0af-3eee-48b4-8407-8694ca628fc0.json"

    def test_serialize(self, small_nodeset, nodeset_multiple_labels, nodeset_multiple_labels_multiple_merge_keys,
                       tmp_path):
        """
        Test serialization with different test NodeSets.
        """

        for test_ns in [small_nodeset, nodeset_multiple_labels, nodeset_multiple_labels_multiple_merge_keys]:
            uuid = 'f8d1f0af-3eee-48b4-8407-8694ca628fc0'
            test_ns.uuid = uuid

            test_ns.serialize(str(tmp_path))

            target_file_path = os.path.join(tmp_path, test_ns.object_file_name(suffix='.json'))

            assert os.path.exists(target_file_path)

            with open(target_file_path, 'rt') as f:
                reloaded_nodeset = NodeSet.from_dict(json.load(f))

                assert reloaded_nodeset.labels == test_ns.labels
                assert reloaded_nodeset.merge_keys == test_ns.merge_keys
                assert reloaded_nodeset.nodes == test_ns.nodes
                assert len(reloaded_nodeset.nodes) == len(test_ns.nodes)


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
