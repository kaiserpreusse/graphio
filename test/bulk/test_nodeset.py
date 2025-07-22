import pytest
import os
import json
from hypothesis import given, strategies as st

from graphio.bulk.nodeset import NodeSet
from graphio import NodeModel

from graphio.utils import run_query_return_results


@pytest.fixture(scope="session")
def reusable_tmp_dir(tmpdir_factory):
    fn = tmpdir_factory.mktemp("data")
    return fn


@pytest.fixture(params=["bare", "NodelNode"])
def small_nodeset(request, test_base) -> NodeSet:
    if request.param == "bare":
        ns = NodeSet(['Test'], merge_keys=['uuid'])
        for i in range(100):
            ns.add_node({'uuid': i, 'key': 'value'})
        yield ns

    elif request.param == "NodelNode":
        class MyNode(NodeModel):
            uuid: str
            _labels = ['Test']
            _merge_keys = ['uuid']


        ns = MyNode.dataset()
        print(ns)

        for i in range(100):
            ns.add_node({'uuid': i, 'key': 'value'})

        yield ns


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




def test_str():
    ns = NodeSet(['Test', 'Foo'], merge_keys=['uuid'])
    assert str(ns) == "<NodeSet (['Test', 'Foo']; ['uuid'])>"


def test_nodeset_add_unique():
    ns = NodeSet(['Test', 'Foo'], merge_keys=['name'])
    for i in range(10):
        ns.add_unique({'name': 'Peter'})
    assert len(ns.nodes) == 1




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


    def test_nodeset_create_additional_labels(self, graph, clear_graph):
        ns = NodeSet(['Test'], merge_keys=['key'], additional_labels=['Foo', 'Bar'])
        for i in range(10):
            ns.add_node({'key': i})

        ns.create(graph)
        result = run_query_return_results(graph, "MATCH (n:Test:Foo:Bar) RETURN count(n)")
        assert result[0][0] == 10

        ns.create(graph)
        result = run_query_return_results(graph, "MATCH (n:Test:Foo:Bar) RETURN count(n)")
        assert result[0][0] == 20

    def test_nodeset_create_without_merge_key(self, graph, clear_graph):
        ns = NodeSet(['Test'])
        for i in range(10):
            ns.add_node({'key': i})

        ns.create(graph)

        result = run_query_return_results(graph, "MATCH (n:Test) RETURN count(n)")
        assert result[0][0] == 10

        ns.create(graph)
        result = run_query_return_results(graph, "MATCH (n:Test) RETURN count(n)")
        assert result[0][0] == 20

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
    def test_nodeset_merge_preserve(self, small_nodeset, graph, clear_graph):
        """
        Merge a nodeset 3 times and check number of nodes.
        """
        ns = small_nodeset

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

    def test_nodeset_merge_additional_labels(self, graph, clear_graph):
        ns = NodeSet(['Test'], merge_keys=['uuid'], additional_labels=['Foo', 'Bar'])
        ns.add_node({'uuid': 1})
        ns2 = NodeSet(['Test'], merge_keys=['uuid'], additional_labels=['Kurt', 'Peter'])
        ns2.add_node({'uuid': 1})

        ns.merge(graph)
        ns.merge(graph)
        ns2.merge(graph)
        ns2.merge(graph)

        result = run_query_return_results(graph, "MATCH (n:Test) RETURN count(n)")
        assert result[0][0] == 1

        result = run_query_return_results(graph, "MATCH (n:Test:Foo:Bar:Kurt:Peter) RETURN count(n)")
        assert result[0][0] == 1

    def test_nodeset_merge_no_merge_keys(self, graph, clear_graph):

        with pytest.raises(ValueError):
            ns = NodeSet(['Test'])
            ns.add_node({'uuid': 1})
            ns.merge(graph)


class TestNodeSetToJSON:

    def test_nodeset_file_name(self, small_nodeset):
        # set fixed uuid for small nodeset
        uuid = 'f8d1f0af-3eee-48b4-8407-8694ca628fc0'
        small_nodeset.uuid = uuid
        assert small_nodeset.object_file_name() == f"nodeset_Test_uuid_f8d1f0af-3eee-48b4-8407-8694ca628fc0"
        assert small_nodeset.object_file_name(
            suffix='.json') == "nodeset_Test_uuid_f8d1f0af-3eee-48b4-8407-8694ca628fc0.json"



