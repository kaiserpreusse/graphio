# note: integration tests for creating relationships needs nodes in the database
# we create the nodes with graphio, this could mean that issues are difficult to resolve
# however, NodeSets are also tested separately
import os
import json
from uuid import uuid4
import pytest
from graphio.bulk.nodeset import NodeSet
from graphio.bulk.relationshipset import RelationshipSet, tuplify_json_list
from graphio import ArrayProperty
from graphio.utils import run_query_return_results


@pytest.fixture
def small_relationshipset():
    rs = RelationshipSet('TEST', ['Test'], ['Foo'], ['uuid'], ['uuid'])

    for i in range(100):
        rs.add_relationship(
            {'uuid': i}, {'uuid': i}, {}
        )

    return rs


@pytest.fixture
def small_relationshipset_no_labels():
    rs = RelationshipSet('TEST', [], [], ['uuid'], ['uuid'])

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
        ns1.add_node({'uuid': i, 'array_key': [i, 9999, 99999]})
        ns2.add_node({'uuid': i, 'array_key': [i, 7777, 77777]})
        ns3.add_node({'uuid': i, 'key': i, 'array_key': [i, 6666, 66666]})

    ns1.create(graph)
    ns2.create(graph)
    ns3.create(graph)

    return ns1, ns2, ns3


def test_str():
    rs = RelationshipSet('TEST', ['Source'], ['Target'], ['uid'], ['name'])

    assert str(rs) == "<RelationshipSet (['Source']; ['uid'])-[TEST]->(['Target']; ['name'])>"




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




class TestDefaultProps:

    def test_default_props(self):
        rs = RelationshipSet('TEST', ['Source'], ['Target'], ['uid'], ['name'], default_props={'user': 'foo'})
        rs.add_relationship({'uid': 1}, {'name': 'peter'}, {'some': 'value'})
        rs.add_relationship({'uid': 2}, {'name': 'tim'}, {'some': 'value'})

        for n in rs.relationships:
            assert n[2]['user'] == 'foo'

    def test_default_props_overwrite_from_node(self):
        rs = RelationshipSet('TEST', ['Source'], ['Target'], ['uid'], ['name'], default_props={'user': 'foo'})
        rs.add_relationship({'uid': 1}, {'name': 'peter'}, {'some': 'value', 'user': 'bar'})
        rs.add_relationship({'uid': 2}, {'name': 'tim'}, {'some': 'value', 'user': 'bar'})

        for r in rs.relationships:
            assert r[2]['user'] == 'bar'


class TestRelationshipSetCreate:

    def test_relationshipset_create_no_labels(self, graph, create_nodes_test, small_relationshipset_no_labels):

        small_relationshipset_no_labels.create(graph)

        # check if 100 relationships are created for two labels
        result = run_query_return_results(graph, "MATCH (t:Test)-[r:TEST]->(f:Foo) RETURN count(r)")

        assert result[0][0] == 100

        # Test that 900 (3*3*100) relationships are created in total
        result = run_query_return_results(graph, "MATCH (t)-[r:TEST]->(f) RETURN count(r)")

        assert result[0][0] == 900

    def test_relationshipset_create_no_properties(self, graph, create_nodes_test):

        rs = RelationshipSet('TEST', ['Test'], ['Foo'], ['uuid'], ['uuid'])

        for i in range(100):
            rs.add_relationship({'uuid': i}, {'uuid': i})

        rs.create(graph)

        result = run_query_return_results(graph, "MATCH (t:Test)-[r:TEST]->(f:Foo) RETURN count(r)")

        assert result[0][0] == 100

    def test_relationshipset_create_number(self, graph, create_nodes_test, small_relationshipset):

        small_relationshipset.create(graph)

        result = run_query_return_results(graph, "MATCH (t:Test)-[r:TEST]->(f:Foo) RETURN count(r)")

        assert result[0][0] == 100

    def test_relationshipset_create_mulitple_node_props(self, graph, create_nodes_test):

        rs = RelationshipSet('TEST', ['Test'], ['Bar'], ['uuid'], ['uuid', 'key'])

        for i in range(100):
            rs.add_relationship(
                {'uuid': i}, {'uuid': i, 'key': i}, {}
            )

        rs.create(graph)

        result = run_query_return_results(graph, "MATCH (:Test)-[r:TEST]->(:Bar) RETURN count(r)")

        assert result[0][0] == 100

    def test_relationshipset_create_array_props(self, graph, create_nodes_test):

        rs = RelationshipSet('TEST_ARRAY', ['Test'], ['Foo'], [ArrayProperty('array_key')], [ArrayProperty('array_key')])

        for i in range(100):
            rs.add_relationship({'array_key': i}, {'array_key': i})

        rs.create(graph)

        result = run_query_return_results(graph, "MATCH (t:Test)-[r:TEST_ARRAY]->(f:Foo) RETURN count(r)")

        assert result[0][0] == 100

    def test_relationshipset_create_string_and_array_props(self, graph, create_nodes_test):

        rs = RelationshipSet('TEST_ARRAY', ['Test'], ['Foo'], [ArrayProperty('array_key')], [ArrayProperty('array_key')])

        for i in range(100):
            rs.add_relationship({'uuid': i, 'array_key': i}, {'uuid': i, 'array_key': i})

        rs.create(graph)

        result = run_query_return_results(graph, "MATCH (t:Test)-[r:TEST_ARRAY]->(f:Foo) RETURN count(r)")

        assert result[0][0] == 100


class TestRelationshipSetIndex:
    def test_relationship_create_single_index(self, graph, clear_graph, small_relationshipset):

        small_relationshipset.create_index(graph)

        # TODO keep until 4.2 is not supported anymore
        try:
            result = run_query_return_results(graph, "SHOW INDEXES YIELD *")
        except:
            result = run_query_return_results(graph, "CALL db.indexes()")

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

        result = run_query_return_results(graph, "MATCH (t:Test)-[r:TEST]->(f:Foo) RETURN count(r)")

        assert result[0][0] == 100

        # merge again to check that number stays the same
        small_relationshipset.merge(graph)

        result = run_query_return_results(graph, "MATCH (t:Test)-[r:TEST]->(f:Foo) RETURN count(r)")

        assert result[0][0] == 100

    def test_relationshipset_merge_no_labels(self, graph, create_nodes_test, small_relationshipset_no_labels):

        small_relationshipset_no_labels.merge(graph)

        # check if 100 relationships are created for two labels
        result = run_query_return_results(graph, "MATCH (t:Test)-[r:TEST]->(f:Foo) RETURN count(r)")

        assert result[0][0] == 100

        # Test that 900 (3*3*100) relationships are created in total
        result = run_query_return_results(graph, "MATCH (t)-[r:TEST]->(f) RETURN count(r)")

        assert result[0][0] == 900

        # merge again to check that number stays the same
        small_relationshipset_no_labels.merge(graph)

        # check if 100 relationships are created for two labels
        result = run_query_return_results(graph, "MATCH (t:Test)-[r:TEST]->(f:Foo) RETURN count(r)")

        assert result[0][0] == 100

        # Test that 900 (3*3*100) relationships are created in total
        result = run_query_return_results(graph, "MATCH (t)-[r:TEST]->(f) RETURN count(r)")

        assert result[0][0] == 900


    def test_relationshipset_merge_array_props(self, graph, create_nodes_test):

        rs = RelationshipSet('TEST_ARRAY', ['Test'], ['Foo'], [ArrayProperty('array_key')], [ArrayProperty('array_key')])

        for i in range(100):
            rs.add_relationship({'array_key': i}, {'array_key': i})

        rs.merge(graph)

        result = run_query_return_results(graph, "MATCH (t:Test)-[r:TEST_ARRAY]->(f:Foo) RETURN count(r)")

        assert result[0][0] == 100

        # merge again
        rs.merge(graph)

        result = run_query_return_results(graph, "MATCH (t:Test)-[r:TEST_ARRAY]->(f:Foo) RETURN count(r)")

        assert result[0][0] == 100

    def test_relationshipset_merge_string_and_array_props(self, graph, create_nodes_test):

        rs = RelationshipSet('TEST_ARRAY', ['Test'], ['Foo'], [ArrayProperty('array_key')], [ArrayProperty('array_key')])

        for i in range(100):
            rs.add_relationship({'uuid': i, 'array_key': i}, {'uuid': i, 'array_key': i})

        rs.merge(graph)

        result = run_query_return_results(graph, "MATCH (t:Test)-[r:TEST_ARRAY]->(f:Foo) RETURN count(r)")

        assert result[0][0] == 100

        # run again
        rs.merge(graph)

        result = run_query_return_results(graph, "MATCH (t:Test)-[r:TEST_ARRAY]->(f:Foo) RETURN count(r)")

        assert result[0][0] == 100

class TestRelationshipSetToJSON:

    def test_object_file_name(self, small_relationshipset):
        # set fixed uuid for relationshipset
        uuid = 'f8d1f0af-3eee-48b4-8407-8694ca628fc0'
        small_relationshipset.uuid = uuid

        assert small_relationshipset.object_file_name() == f"relationshipset_Test_TEST_Foo_f8d1f0af-3eee-48b4-8407-8694ca628fc0"
        assert small_relationshipset.object_file_name(
            suffix='.json') == "relationshipset_Test_TEST_Foo_f8d1f0af-3eee-48b4-8407-8694ca628fc0.json"





