# note: integration tests for creating relationships needs nodes in the database
# we create the nodes with graphio, this could mean that issues are difficult to resolve
# however, NodeSets are also tested separately
import os
import json
import pytest
from graphio.objects.nodeset import NodeSet
from graphio.objects.relationshipset import RelationshipSet, tuplify_json_list


@pytest.fixture
def small_relationshipset():
    rs = RelationshipSet('TEST', ['Test'], ['Foo'], ['uuid'], ['uuid'])

    for i in range(100):
        rs.add_relationship(
            {'uuid': i}, {'uuid': i}, {}
        )

    return rs


@pytest.fixture
def small_relationshipset_multiple_labels():
    rs = RelationshipSet('TEST', ['Test', 'Other'], ['Foo', 'SomeLabel'], ['uuid'], ['uuid'])

    for i in range(100):
        rs.add_relationship(
            {'uuid': i}, {'uuid': i}, {}
        )

    return rs


@pytest.fixture
def small_relationshipset_multiple_labels_multiple_merge_keys():
    rs = RelationshipSet('TEST', ['Test', 'Other'], ['Foo', 'SomeLabel'], ['uuid', 'numerical'], ['uuid', 'value'])

    for i in range(100):
        rs.add_relationship(
            {'uuid': i, 'numerical': 1}, {'uuid': i, 'value': 'foo'}, {}
        )

    return rs


@pytest.fixture(scope='function')
def create_nodes_test(graph, clear_graph):
    ns1 = NodeSet(['Test'], merge_keys=['uuid'])
    ns2 = NodeSet(['Foo'], merge_keys=['uuid'])
    ns3 = NodeSet(['Bar'], merge_keys=['uuid', 'key'])

    for i in range(100):
        ns1.add_node({'uuid': i})
        ns2.add_node({'uuid': i})
        ns3.add_node({'uuid': i, 'key': i})

    ns1.create(graph)
    ns2.create(graph)
    ns3.create(graph)

    return ns1, ns2, ns3


def test_str():
    rs = RelationshipSet('TEST', ['Source'], ['Target'], ['uid'], ['name'])

    assert str(rs) == "<RelationshipSet (['Source']; ['uid'])-[TEST]->(['Target']; ['name'])>"


def test_relationship_set_from_dict():
    rs = RelationshipSet('TEST', ['Source'], ['Target'], ['uid'], ['name'])
    rs.add_relationship({'uid': 1}, {'name': 'peter'}, {})
    rs.add_relationship({'uid': 2}, {'name': 'tim'}, {})

    rs_dictionary = rs.to_dict()

    rs_copy = RelationshipSet.from_dict(rs_dictionary)
    assert rs_copy.to_dict() == rs_dictionary


def test__tuplify_json_list():
    l = [[0, 1], {}, [0, 'foo']]

    t = tuplify_json_list(l)

    assert t == ((0, 1), {}, (0, 'foo'))


def test_relationshipset_unique():
    rs = RelationshipSet('TEST', ['Source'], ['Target'], ['uid'], ['name'])
    rs.unique = True
    for i in range(10):
        rs.add_relationship({'uid': 1}, {'name': 'peter'}, {'some': 'value', 'user': 'bar'})
    assert len(rs.relationships) == 1


def test_relationshipset_all_property_keys():
    rs = RelationshipSet('TEST', ['Source'], ['Target'], ['uid'], ['name'])

    random_keys = ['name', 'city', 'value', 'key']

    for val in random_keys:
        for i in range(20):
            rel_props = {}
            rel_props[val] = 'peter'

            rs.add_relationship({'uid': 1}, {'name': 'peter'}, rel_props)

    assert rs.all_property_keys() == set(random_keys)


def test_relationshipset_estiamte_types():
    rs = RelationshipSet('TEST', ['Source'], ['Target'], ['uid'], ['name', 'height', 'age'])

    for i in range(20):
        rs.add_relationship({'uid': 1}, {'name': 'peter', 'height': 20.5, 'age': 50},
                            {'country': 'germany', 'weight': 70.4, 'number': 20})

    start_node_property_types, rel_property_types, end_node_property_types = rs._estimate_type_of_property_values()
    assert start_node_property_types['uid'] == int
    assert end_node_property_types['name'] == str
    assert end_node_property_types['height'] == float
    assert end_node_property_types['age'] == int
    assert rel_property_types['country'] == str
    assert rel_property_types['weight'] == float
    assert rel_property_types['number'] == int


class TestDefaultProps:

    def test_default_props(self):
        rs = RelationshipSet('TEST', ['Source'], ['Target'], ['uid'], ['name'], default_props={'user': 'foo'})
        rs.add_relationship({'uid': 1}, {'name': 'peter'}, {'some': 'value'})
        rs.add_relationship({'uid': 2}, {'name': 'tim'}, {'some': 'value'})

        for n in rs.relationships:
            assert n[1]['user'] == 'foo'

    def test_default_props_overwrite_from_node(self):
        rs = RelationshipSet('TEST', ['Source'], ['Target'], ['uid'], ['name'], default_props={'user': 'foo'})
        rs.add_relationship({'uid': 1}, {'name': 'peter'}, {'some': 'value', 'user': 'bar'})
        rs.add_relationship({'uid': 2}, {'name': 'tim'}, {'some': 'value', 'user': 'bar'})

        for r in rs.relationships:
            assert r[1]['user'] == 'bar'


class TestRelationshipSetCreate:

    def test_relationshipset_create_no_properties(self, graph, create_nodes_test):

        rs = RelationshipSet('TEST', ['Test'], ['Foo'], ['uuid'], ['uuid'])

        for i in range(100):
            rs.add_relationship({'uuid': i}, {'uuid': i})

        rs.create(graph)

        result = list(
            graph.run(
                "MATCH (t:Test)-[r:TEST]->(f:Foo) RETURN count(r)"
            )
        )
        assert result[0][0] == 100

    def test_relationshipset_create_number(self, graph, create_nodes_test, small_relationshipset):

        small_relationshipset.create(graph)

        result = list(
            graph.run(
                "MATCH (t:Test)-[r:TEST]->(f:Foo) RETURN count(r)"
            )
        )
        assert result[0][0] == 100

    def test_relationshipset_create_mulitple_node_props(self, graph, create_nodes_test):

        rs = RelationshipSet('TEST', ['Test'], ['Bar'], ['uuid'], ['uuid', 'key'])

        for i in range(100):
            rs.add_relationship(
                {'uuid': i}, {'uuid': i, 'key': i}, {}
            )

        rs.create(graph)

        result = list(
            graph.run(
                "MATCH (:Test)-[r:TEST]->(:Bar) RETURN count(r)"
            )
        )
        print(result)
        print(result[0])
        assert result[0][0] == 100


class TestRelationshipSetIndex:
    def test_relationship_create_single_index(self, graph, clear_graph, small_relationshipset):

        small_relationshipset.create_index(graph)

        result = list(
            graph.run("CALL db.indexes()")
        )

        for row in result:
            # the result of the db.indexes() procedure is different for Neo4j 3.5 and 4
            # this should also be synced with differences in py2neo versions
            if 'tokenNames' in row:
                assert row['tokenNames'] == ['Test'] and row['properties'] == ['uuid'] \
                       or row['tokenNames'] == ['Test'] and row['properties'] == ['uuid']

            elif 'labelsOrTypes' in row:
                assert row['labelsOrTypes'] == ['Test'] and row['properties'] == ['uuid'] \
                       or row['labelsOrTypes'] == ['Test'] and row['properties'] == ['uuid']


class TestRelationshipSetMerge:

    def test_relationshipset_merge_number(self, graph, create_nodes_test, small_relationshipset):
        small_relationshipset.merge(graph)

        result = list(
            graph.run(
                "MATCH (t:Test)-[r:TEST]->(f:Foo) RETURN count(r)"
            )
        )
        print(result)
        print(result[0])
        assert result[0][0] == 100

        # merge again to check that number stays the same
        small_relationshipset.merge(graph)

        result = list(
            graph.run(
                "MATCH (t:Test)-[r:TEST]->(f:Foo) RETURN count(r)"
            )
        )
        print(result)
        print(result[0])
        assert result[0][0] == 100


class TestRelationshipSetSerialize:

    def test_object_file_name(self, small_relationshipset):
        # set fixed uuid for relationshipset
        uuid = 'f8d1f0af-3eee-48b4-8407-8694ca628fc0'
        small_relationshipset.uuid = uuid

        assert small_relationshipset.object_file_name() == f"relationshipset_Test_TEST_Foo_f8d1f0af-3eee-48b4-8407-8694ca628fc0"
        assert small_relationshipset.object_file_name(
            suffix='.json') == "relationshipset_Test_TEST_Foo_f8d1f0af-3eee-48b4-8407-8694ca628fc0.json"

    def test_serialize(self, small_relationshipset, small_relationshipset_multiple_labels,
                       small_relationshipset_multiple_labels_multiple_merge_keys, tmp_path):
        """
        Test serialization with different test NodeSets.
        """

        for test_rs in [small_relationshipset, small_relationshipset_multiple_labels,
                        small_relationshipset_multiple_labels_multiple_merge_keys]:
            uuid = 'f8d1f0af-3eee-48b4-8407-8694ca628fc0'
            test_rs.uuid = uuid

            test_rs.serialize(str(tmp_path))

            target_file_path = os.path.join(tmp_path, test_rs.object_file_name(suffix='.json'))

            assert os.path.exists(target_file_path)

            with open(target_file_path, 'rt') as f:
                reloaded_relset = RelationshipSet.from_dict(json.load(f))

                assert reloaded_relset.start_node_labels == test_rs.start_node_labels
                assert reloaded_relset.start_node_properties == test_rs.start_node_properties
                assert reloaded_relset.end_node_labels == test_rs.end_node_labels
                assert reloaded_relset.end_node_properties == test_rs.end_node_properties
                assert reloaded_relset.relationships == test_rs.relationships
                assert len(reloaded_relset.relationships) == len(test_rs.relationships)
