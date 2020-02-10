# note: integration tests for creating relationships needs nodes in the database
# we create the nodes with graphio, this could mean that issues are difficult to resolve
# however, NodeSets are also tested separately

import pytest
from graphio.objects.nodeset import NodeSet, RelationshipSet


@pytest.fixture
def create_nodes_test(graph, clear_graph):
    ns1 = NodeSet(['Test'], merge_keys=['uuid'])
    ns2 = NodeSet(['Foo'], merge_keys=['uuid'])

    for i in range(100):
        ns1.add_node({'uuid': i})
        ns2.add_node({'uuid': i})

    ns1.create(graph)
    ns2.create(graph)

    return ns1, ns2


class TestRelationshipSet:

    def test_relationshipset_create_number(self, graph, create_nodes_test):

        rs = RelationshipSet('TEST', ['Test'], ['Foo'], ['uuid'], ['uuid'])

        for i in range(10):
            rs.add_relationship(
                {'uuid': i}, {'uuid': i}, {}
            )

        rs.create(graph)

        result = list(
            graph.run(
                "MATCH (t:Test)-[r:TEST]->(f:Foo) RETURN count(r)"
            )
        )
        print(result)
        print(result[0])
        assert result[0][0] == 10
