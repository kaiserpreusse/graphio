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

        with graph.session() as s:

            result = list(
                s.run(
                    "MATCH (n:{}) RETURN count(n)".format(':'.join(small_nodeset.labels))
                )
            )
            print(result)
            assert result[0][0] == 100

    def test_nodeset_create_twice_number(self, small_nodeset, graph, clear_graph):
        small_nodeset.create(graph)
        small_nodeset.create(graph)

        with graph.session() as s:

            result = list(
                s.run(
                    "MATCH (n:{}) RETURN count(n)".format(':'.join(small_nodeset.labels))
                )
            )
            print(result)
            assert result[0][0] == 200

    def test_nodeset_create_properties(self, small_nodeset, graph, clear_graph):
        small_nodeset.create(graph)

        with graph.session() as s:
            result = list(
                s.run(
                    "MATCH (n:{}) RETURN n".format(':'.join(small_nodeset.labels))
                )
            )

            for row in result:
                node = row[0]
                assert node['key'] == 'value'

    def test_create_nodeset_multiple_labels(self, nodeset_multiple_labels, graph, clear_graph):
        nodeset_multiple_labels.create(graph)
        with graph.session() as s:
            result = list(
                s.run(
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

        with graph.session() as s:
            result = list(
                s.run("CALL db.indexes()")
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

        with graph.session() as s:

            result = list(
                s.run("CALL db.indexes()")
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
    def test_nodeset_merge_number(self, small_nodeset, graph, clear_graph):
        """
        Merge a nodeset 3 times and check number of nodes.
        """
        small_nodeset.merge(graph)
        small_nodeset.merge(graph)
        small_nodeset.merge(graph)

        with graph.session() as s:

            result = list(
                s.run("MATCH (n:{}) RETURN count(n)".format(':'.join(small_nodeset.labels)))
            )

            print(result)
            assert result[0][0] == 100
