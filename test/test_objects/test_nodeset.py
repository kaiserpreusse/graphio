import pytest
import os
import json
from hypothesis import given, strategies as st

from graphio.objects.nodeset import NodeSet


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
