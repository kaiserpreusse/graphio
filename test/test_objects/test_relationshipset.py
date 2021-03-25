# note: integration tests for creating relationships needs nodes in the database
# we create the nodes with graphio, this could mean that issues are difficult to resolve
# however, NodeSets are also tested separately

import pytest
from graphio.objects.nodeset import NodeSet, RelationshipSet


@pytest.fixture
def small_relationshipset():
    rs = RelationshipSet('TEST', ['Test'], ['Foo'], ['uuid'], ['uuid'])

    for i in range(100):
        rs.add_relationship(
            {'uuid': i}, {'uuid': i}, {}
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


def test_relationshuo_set_from_dict():
    rs = RelationshipSet('TEST', ['Source'], ['Target'], ['uid'], ['name'])
    rs.add_relationship({'uid': 1}, {'name': 'peter'}, {})
    rs.add_relationship({'uid': 2}, {'name': 'tim'}, {})

    rs_dictionary = rs.to_dict()

    rs_copy = RelationshipSet.from_dict(rs_dictionary)
    assert rs_copy.to_dict() == rs_dictionary


class TestRelationshipSetCreate:

    def test_relationshipset_create_number(self, graph, create_nodes_test, small_relationshipset):

        small_relationshipset.create(graph)

        result = list(
            graph.run(
                "MATCH (t:Test)-[r:TEST]->(f:Foo) RETURN count(r)"
            )
        )
        print(result)
        print(result[0])
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
