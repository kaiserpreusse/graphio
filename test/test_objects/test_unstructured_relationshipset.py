from graphio.objects.unstructured_relationshipset import UnstructuredRelationshipSet, Relationship, NodeMatch
from graphio.objects.unstructured_nodeset import UnstructuredNodeSet, Node
from graphio.graph import run_query_return_results


def test_unstructred_relationship_unique_node_definitions():
    urs = UnstructuredRelationshipSet()
    urs.add_relationship(Relationship(start_node=NodeMatch(labels=["A"], properties={"a": 1}),
                                      end_node=NodeMatch(labels=["B"], properties={"b": 2}), type="REL",
                                      properties={"c": 3}))
    urs.add_relationship(Relationship(start_node=NodeMatch(labels=["C"], properties={"c": 1}),
                                      end_node=NodeMatch(labels=["D"], properties={"d": 2}), type="REL",
                                      properties={"c": 3}))

    assert urs.unique_node_definitions == {(('D',), ('d',)), (('B',), ('b',)), (('C',), ('c',)), (('A',), ('a',))}


class TestUnstructuredRelationshipSetIndexes:

    def test_create_single_indexes(self, graph, clear_graph):
        labels = ["A"]
        properties = ["a"]
        urs = UnstructuredRelationshipSet()
        urs.add_relationship(Relationship(start_node=NodeMatch(labels=labels, properties={"a": 1}),
                                          end_node=NodeMatch(labels=labels, properties={"a": 1}), type="REL",
                                          properties={"c": 3}))

        urs.create_index(graph)

        # TODO keep until 4.2 is not supported anymore
        try:
            result = run_query_return_results(graph, "SHOW INDEXES YIELD *")
        except:
            result = run_query_return_results(graph, "CALL db.indexes()")

        for row in result:
            if 'tokenNames' in row:
                assert row['tokenNames'] == labels and row['properties'] == properties \
                       or row['tokenNames'] == labels and row['properties'] == properties

            elif 'labelsOrTypes' in row:
                assert row['labelsOrTypes'] == labels and row['properties'] == properties \
                       or row['labelsOrTypes'] == labels and row['properties'] == properties

    def test_create_composite_indexes(self, graph, clear_graph):
        labels = ["A"]
        properties = ["a", "b"]
        urs = UnstructuredRelationshipSet()
        urs.add_relationship(Relationship(start_node=NodeMatch(labels=labels, properties={"a": 1, "b": 2}),
                                          end_node=NodeMatch(labels=labels, properties={"a": 1, "b": 2}), type="REL",
                                          properties={"c": 3}))

        urs.create_index(graph)

        # TODO keep until 4.2 is not supported anymore
        try:
            result = run_query_return_results(graph, "SHOW INDEXES YIELD *")
        except:
            result = run_query_return_results(graph, "CALL db.indexes()")

        for row in result:
            if 'tokenNames' in row:
                assert row['tokenNames'] == labels and row['properties'] == properties \
                       or row['tokenNames'] == labels and row['properties'] == properties

            elif 'labelsOrTypes' in row:
                assert row['labelsOrTypes'] == labels and row['properties'] == properties \
                       or row['labelsOrTypes'] == labels and row['properties'] == properties


class TestUnstructuredRelationshipSetCreate:

        def test_create_relationships(self, graph, clear_graph):

            uns = UnstructuredNodeSet()
            uns.add_node(Node(labels=["A"], merge_keys=["a"], properties={"a": 1}))
            uns.add_node(Node(labels=["B"], merge_keys=["b"], properties={"b": 2}))
            uns.add_node(Node(labels=["C"], merge_keys=["c"], properties={"c": 1}))
            uns.add_node(Node(labels=["D"], merge_keys=["d"], properties={"d": 2}))

            uns.create(graph)

            urs = UnstructuredRelationshipSet()
            urs.add_relationship(Relationship(start_node=NodeMatch(labels=["A"], properties={"a": 1}),
                                            end_node=NodeMatch(labels=["B"], properties={"b": 2}), type="REL",
                                            properties={"c": 3}))
            urs.add_relationship(Relationship(start_node=NodeMatch(labels=["C"], properties={"c": 1}),
                                            end_node=NodeMatch(labels=["D"], properties={"d": 2}), type="REL",
                                            properties={"c": 3}))

            urs.create(graph)

            result = run_query_return_results(graph, "MATCH (n)-[r]->(m) RETURN n, r, m")

            assert len(result) == 2
            assert result[0]['n']['a'] == 1
            assert result[0]['m']['b'] == 2
            assert result[0]['r']['c'] == 3
            assert result[1]['n']['c'] == 1
            assert result[1]['m']['d'] == 2
            assert result[1]['r']['c'] == 3

        def test_merge_relationships(self, graph, clear_graph):

            uns = UnstructuredNodeSet()
            uns.add_node(Node(labels=["A"], merge_keys=["a"], properties={"a": 1}))
            uns.add_node(Node(labels=["B"], merge_keys=["b"], properties={"b": 2}))
            uns.add_node(Node(labels=["C"], merge_keys=["c"], properties={"c": 1}))
            uns.add_node(Node(labels=["D"], merge_keys=["d"], properties={"d": 2}))

            uns.create(graph)

            urs = UnstructuredRelationshipSet()
            urs.add_relationship(Relationship(start_node=NodeMatch(labels=["A"], properties={"a": 1}),
                                            end_node=NodeMatch(labels=["B"], properties={"b": 2}), type="REL",
                                            properties={"c": 3}))
            urs.add_relationship(Relationship(start_node=NodeMatch(labels=["C"], properties={"c": 1}),
                                            end_node=NodeMatch(labels=["D"], properties={"d": 2}), type="REL",
                                            properties={"c": 3}))

            urs.merge(graph)
            urs.merge(graph)
            urs.merge(graph)

            result = run_query_return_results(graph, "MATCH (n)-[r]->(m) RETURN n, r, m")

            assert len(result) == 2
            assert result[0]['n']['a'] == 1
            assert result[0]['m']['b'] == 2
            assert result[0]['r']['c'] == 3
            assert result[1]['n']['c'] == 1
            assert result[1]['m']['d'] == 2
            assert result[1]['r']['c'] == 3