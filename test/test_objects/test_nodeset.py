import pytest

from graphio.objects.nodeset import NodeSet


@pytest.fixture
def small_nodeset():
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


class TestNodeSetCreate:

    def test_nodeset_create_number(self, small_nodeset, graph, clear_graph):
        small_nodeset.create(graph)

        result = list(
            graph.run(
                "MATCH (n:{}) RETURN count(n)".format(':'.join(small_nodeset.labels))
            )
        )

        assert result[0][0] == 100

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
