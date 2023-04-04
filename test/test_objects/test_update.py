from uuid import UUID

import pytest

from graphio.objects.update import GraphUpdate, RelationshipSet, NodeSet


@pytest.fixture
def small_relationshipset():
    rs = RelationshipSet('TEST', ['Test'], ['Foo'], ['uuid'], ['uuid'])

    for i in range(100):
        rs.add_relationship(
            {'uuid': i}, {'uuid': i}, {}
        )

    return rs


@pytest.fixture
def matching_nodesets():
    ns1 = NodeSet(['Test'], merge_keys=['uuid'])
    ns2 = NodeSet(['Foo'], merge_keys=['uuid'])

    for i in range(100):
        ns1.add_node({'uuid': i, 'array_key': [i, 9999, 99999]})
        ns2.add_node({'uuid': i, 'array_key': [i, 7777, 77777]})

    return ns1, ns2


class TestGraphUpdate:

    def test_graph_update_instance(self):
        graph_update = GraphUpdate()
        assert graph_update is not None
        assert isinstance(graph_update, GraphUpdate)
        assert graph_update.uuid is not None
        assert isinstance(graph_update.uuid, str)


class TestGraphUpdateCycle:

    def test_graph_update(self, graph, clear_graph, small_relationshipset, matching_nodesets):
        ns1, ns2 = matching_nodesets

        graph_update = GraphUpdate()
        graph_update.start(graph)

        ns1.merge(graph)
        graph_update.add_nodeset(ns1.to_definition())

        ns2.merge(graph)
        graph_update.add_nodeset(ns2.to_definition())

        small_relationshipset.merge(graph)
        graph_update.add_relationshipset(small_relationshipset.to_definition())

        graph_update.finish()

        with graph.session() as s:
            assert list(s.run("MATCH (n:GraphUpdate) RETURN n.uuid as uuid"))[0][0] == graph_update.uuid
            assert list(s.run("MATCH (n:NodeSet) RETURN count(n)"))[0][0] == 2
            assert list(s.run("MATCH (n:RelationshipSet) RETURN count(n)"))[0][0] == 1
            assert list(s.run("MATCH (g:GraphUpdate)-[:CONTAINS]->(n:RelationshipSet) RETURN count(n)"))[0][0] == 1
            assert list(s.run("MATCH (g:GraphUpdate)-[:CONTAINS]->(n:NodeSet) RETURN count(n)"))[0][0] == 2
