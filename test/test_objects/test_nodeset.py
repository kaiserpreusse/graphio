import pytest
import os
import json
from hypothesis import given, strategies as st

from graphio.objects.nodeset import NodeSet
from graphio.graph import run_query_return_results


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
def nodeset_no_label() -> NodeSet:
    ns = NodeSet(merge_keys=['uuid'])
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


def test_nodeset_merge_key_id():

    ns = NodeSet(['Test'], ['name', 'foo'])

    merge_key_id = ns._merge_key_id({'name': 'Peter', 'foo': 'bar'})
    assert merge_key_id == ('Peter', 'bar')


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


class TestNodeIndexOnNodeSet:

    def test_index_creation(self):
        ns = NodeSet(['Test'], ['name', 'id'], indexed=True)
        for i in range(1, 101):
            # add node twice to index
            ns.add_node({'name': str(i), 'id': i})
            ns.add_node({'name': str(i), 'id': i})
        # assert that the positions match as expected
        for i in range(1, 101):
            index = (str(i), i)
            assert index in ns.node_index
            assert ns.node_index[index] == [(i-1)*2, ((i-1)*2)+1]


class TestNodeSetNodeUpdate:
    """
    Test the function to update existing nodes in a NodeSet.
    """
    def test_error_on_non_indexed(self):
        ns = NodeSet(['Test'], ['name'])
        with pytest.raises(TypeError):
            ns.update_node({'name': 'Peter', 'age': 60, 'city': 'Munich'})

    def test_update_nodes(self):
        ns = NodeSet(['Test'], ['name'], indexed=True)
        ns.add_node({'name': 'Peter', 'age': 50})
        ns.update_node({'name': 'Peter', 'age': 60, 'city': 'Munich'})
        assert len(ns.nodes) == 1
        assert ns.nodes[0] == {'name': 'Peter', 'age': 60, 'city': 'Munich'}


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

        result = run_query_return_results(graph, "MATCH (n:{}) RETURN count(n)".format(':'.join(small_nodeset.labels)))

        assert result[0][0] == 100

    def test_nodeset_create_twice_number(self, small_nodeset, graph, clear_graph):
        small_nodeset.create(graph)
        small_nodeset.create(graph)

        result = run_query_return_results(graph, "MATCH (n:{}) RETURN count(n)".format(':'.join(small_nodeset.labels)))
        print(result)
        assert result[0][0] == 200

    def test_nodeset_create_properties(self, small_nodeset, graph, clear_graph):
        small_nodeset.create(graph)

        result = run_query_return_results(graph, "MATCH (n:{}) RETURN n".format(':'.join(small_nodeset.labels)))

        for row in result:
            node = row[0]
            assert node['key'] == 'value'

    def test_create_nodeset_multiple_labels(self, nodeset_multiple_labels, graph, clear_graph):
        nodeset_multiple_labels.create(graph)

        result = run_query_return_results(graph, "MATCH (n:{}) RETURN count(n)".format(':'.join(nodeset_multiple_labels.labels)))

        assert result[0][0] == 100

    def test_nodeset_create_no_label(self, nodeset_no_label, graph, clear_graph):
        nodeset_no_label.create(graph)

        result = run_query_return_results(graph, "MATCH (n) RETURN count(n)")

        assert result[0][0] == 100


class TestNodeSetIndex:

    def test_nodeset_create_single_index(self, graph, clear_graph):
        labels = ['TestNode']
        properties = ['some_key']
        ns = NodeSet(labels, merge_keys=properties)

        ns.create_index(graph)

        # TODO keep until 4.2 is not supported anymore
        try:
            result = run_query_return_results(graph, "SHOW INDEXES YIELD *")
        except:
            result = run_query_return_results(graph, "CALL db.indexes()")

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

        # TODO keep until 4.2 is not supported anymore
        try:
            result = run_query_return_results(graph, "SHOW INDEXES YIELD *")
        except:
            result = run_query_return_results(graph, "CALL db.indexes()")

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

        assert run_query_return_results(graph, "MATCH (n:Test) where n.key = 'value' RETURN count(n)")[0][0] == 100
        assert run_query_return_results(graph, "MATCH (n:Test) where n.key = 'other_value' RETURN count(n)")[0][0] == 0

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
        assert run_query_return_results(graph, "MATCH (n:Test) where 'value' in n.key and 'other_value' in n.key RETURN count(n)")[0][
                   0] == 100

    def test_nodeset_merge_preserve_and_append_props(self, graph, clear_graph):
        """
        Merge a nodeset 3 times and check number of nodes.
        """
        ns = NodeSet(['Test'], merge_keys=['uuid'], append_props=['key'], preserve=['other_key'])
        for i in range(100):
            ns.add_node({'uuid': i, 'key': 'value', 'other_key': 'bar'})

        ns.merge(graph)
        assert run_query_return_results(graph, "MATCH (n:Test) where 'value' IN n.key RETURN count(n)")[0][0] == 100
        assert run_query_return_results(graph, "MATCH (n:Test) where n.other_key = 'bar' RETURN count(n)")[0][0] == 100

        append_ns = NodeSet(['Test'], merge_keys=['uuid'], append_props=['key'], preserve=['other_key'])
        for i in range(100):
            append_ns.add_node({'uuid': i, 'key': 'other_value', 'other_key': 'foo'})

        append_ns.merge(graph)

        assert run_query_return_results(graph, "MATCH (n:Test) where 'value' in n.key and 'other_value' in n.key RETURN count(n)")[0][
                   0] == 100
        assert run_query_return_results(graph, "MATCH (n:Test) where n.other_key = 'bar' RETURN count(n)")[0][0] == 100
        assert run_query_return_results(graph, "MATCH (n:Test) where n.other_key = 'foo' RETURN count(n)")[0][0] == 0

    def test_nodeset_merge_preserve_keeps_append_props(self, graph, clear_graph):
        """
        Merge a nodeset 3 times and check number of nodes.
        """
        ns = NodeSet(['Test'], merge_keys=['uuid'], append_props=['key'], preserve=['key'])
        for i in range(100):
            ns.add_node({'uuid': i, 'key': 'value'})

        ns.merge(graph)
        assert run_query_return_results(graph, "MATCH (n:Test) where 'value' IN n.key RETURN count(n)")[0][0] == 100

        append_ns = NodeSet(['Test'], merge_keys=['uuid'], append_props=['key'], preserve=['key'])
        for i in range(100):
            append_ns.add_node({'uuid': i, 'key': 'other_value'})

        append_ns.merge(graph)

        assert run_query_return_results(graph, "MATCH (n:Test) where 'value' IN n.key RETURN count(n)")[0][0] == 100
        assert run_query_return_results(graph, "MATCH (n:Test) where 'other_value' IN n.key RETURN count(n)")[0][0] == 0

    def test_nodeset_merge_number(self, small_nodeset, graph, clear_graph):
        """
        Merge a nodeset 3 times and check number of nodes.
        """
        small_nodeset.merge(graph)
        small_nodeset.merge(graph)
        small_nodeset.merge(graph)

        result = run_query_return_results(graph, "MATCH (n:{}) RETURN count(n)".format(':'.join(small_nodeset.labels)))

        assert result[0][0] == 100

    def test_nodeset_merge_no_label(self, nodeset_no_label, graph, clear_graph):
        nodeset_no_label.merge(graph)
        nodeset_no_label.merge(graph)

        result = run_query_return_results(graph, "MATCH (n) RETURN count(n)")

        assert result[0][0] == 100


class TestNodeSetToJSON:

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

            test_ns.to_json(str(tmp_path))

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
            filepath = os.path.join(tmp_path, 'nodeset.csv')
            num_of_props = len(ns.all_property_keys())

            csv_file = ns.to_csv(filepath)

            with open(csv_file) as f:
                lines = f.readlines()
                # note that csv has header
                assert len(lines) - 1 == len(ns.nodes)

                for l in lines:
                    l = l.strip()
                    assert len(l.split(',')) == num_of_props

    def test_create_csv_file_header(self, different_nodesets, tmp_path):
        for ns in different_nodesets:
            filepath = os.path.join(tmp_path, 'nodeset.csv')
            csv_file = ns.to_csv(filepath)

            with open(csv_file) as f:
                lines = f.readlines()
                # note that csv has header
                assert len(lines) - 1 == len(ns.nodes)

                header = lines[0].strip().split(',')
                assert set(header) == set(ns.all_property_keys())

    def test_create_csv_query(self, small_nodeset):
        # override uuid which is used in file name
        small_nodeset.uuid = 'peter'
        query = small_nodeset.create_csv_query()
        print(query)
        assert query == """USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///nodeset_Test_uuid_peter.csv' AS line
CREATE (n:Test)
SET n.key = line.key, n.uuid = toInteger(line.uuid)"""

    def test_merge_csv_query(self, small_nodeset):
        # override uuid
        small_nodeset.uuid = 'peter'
        query = small_nodeset.merge_csv_query()

        assert query == """USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///nodeset_Test_uuid_peter.csv' AS line
MERGE (n:Test { uuid: toInteger(line.uuid) })
SET n.key = line.key, n.uuid = toInteger(line.uuid)"""


class TestNodeSetCSVandJSON:
    """
    Test functionality around creating/reading CSV files and associated JSON metadata files.
    """
    def test_read_from_files(self, root_dir, clear_graph, graph):

        files_path = os.path.join(root_dir, "test", "files")

        json_file_path = os.path.join(files_path, 'nodes_csv_json.json')
        csv_file_path = os.path.join(files_path, 'nodes_csv_json.csv')

        assert os.path.exists(json_file_path)
        assert os.path.exists(csv_file_path)

        ns = NodeSet.from_csv_json_set(csv_file_path, json_file_path)
        assert ns.labels == ['Test']
        assert ns.merge_keys == ['test_id']

        ns.merge(graph)
        assert run_query_return_results(graph, "MATCH (n:Test) RETURN count(n)")[0][0] == 2
        assert isinstance(run_query_return_results(graph, "MATCH (n:Test) RETURN n.test_id")[0][0], int)

    def test_read_from_files_load_items_to_memory(self, root_dir, clear_graph, graph):

        files_path = os.path.join(root_dir, "test", "files")

        json_file_path = os.path.join(files_path, 'nodes_csv_json.json')
        csv_file_path = os.path.join(files_path, 'nodes_csv_json.csv')

        assert os.path.exists(json_file_path)
        assert os.path.exists(csv_file_path)

        ns = NodeSet.from_csv_json_set(csv_file_path, json_file_path, load_items=True)
        assert ns.labels == ['Test']
        assert ns.merge_keys == ['test_id']
        for n in ns.nodes:
            assert isinstance(n['test_id'], int)

        ns.merge(graph)
        assert run_query_return_results(graph, "MATCH (n:Test) RETURN count(n)")[0][0] == 2

    def test_write_files_read_again_load_items_into_memory(self, small_nodeset, tmp_path, clear_graph, graph):
        """
        Test writing files, reading them again and loading items from files.
        """
        csv_file_path = os.path.join(tmp_path, 'test_nodeset.csv')
        json_file_path = os.path.join(tmp_path, 'test_nodeset.json')

        small_nodeset.to_csv_json_set(csv_file_path, json_file_path, type_conversion={'uuid': 'int'})

        assert os.path.exists(json_file_path)
        assert os.path.exists(csv_file_path)

        ns = NodeSet.from_csv_json_set(csv_file_path, json_file_path, load_items=True)
        assert ns.labels == ['Test']
        assert ns.merge_keys == ['uuid']
        assert len(ns.nodes) == len(small_nodeset.nodes)
        for n in ns.nodes:
            assert isinstance(n['uuid'], int)

        ns.merge(graph)
        assert run_query_return_results(graph, "MATCH (n:Test) RETURN count(n)")[0][0] == len(small_nodeset.nodes)