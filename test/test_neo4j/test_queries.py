from graphio.queries import nodes_create_unwind, nodes_merge_unwind


class TestNodesCreateUnwind:

    def test_single_label(self):
        test_labels = ['Foo']
        query = nodes_create_unwind(test_labels)
        assert query == 'UNWIND $props AS properties CREATE (n:Foo) SET n = properties'

    def test_mulitple_labels(self):
        test_labels = ['Foo', 'Bar']
        query = nodes_create_unwind(test_labels)
        assert query == 'UNWIND $props AS properties CREATE (n:Foo:Bar) SET n = properties'

    def test_own_param_name(self):
        test_labels = ['Foo']
        test_param_name = 'nodes'
        query = nodes_create_unwind(test_labels, property_parameter=test_param_name)
        assert query == 'UNWIND ${0} AS properties CREATE (n:{1}) SET n = properties'.format(test_param_name,
                                                                                             test_labels[0])

    def test_query_creates_nodes(self, graph, clear_graph):
        query = nodes_create_unwind(['Foo', 'Bar'])

        graph.run(query, props=[{'testid': 1}, {'testid': 2}])

        result = graph.run('MATCH (n:Foo:Bar) RETURN n.testid AS testid')

        collected_ids = set()

        for row in result:
            assert row['testid']
            collected_ids.add(row['testid'])

        assert collected_ids == {1, 2}


class TestNodesMergeUnwind:

    def test_query_single_label(self):
        test_label = ['Foo']

        query = nodes_merge_unwind(test_label, ['sid'])

        expected_query = """UNWIND $props AS properties
MERGE (n:Foo { sid: properties.sid } )
ON CREATE SET n = properties
ON MATCH SET n += properties"""

        assert query == expected_query

    def test_query_multiple_labels(self):
        test_labels = ['Foo', 'Bar']

        query = nodes_merge_unwind(test_labels, ['sid'])

        expected_query = """UNWIND $props AS properties
MERGE (n:Foo:Bar { sid: properties.sid } )
ON CREATE SET n = properties
ON MATCH SET n += properties"""

        assert query == expected_query

    def test_query_multiple_merge_properties(self):
        test_labels = ['Foo', 'Bar']
        merge_props = ['sid', 'other']

        query = nodes_merge_unwind(test_labels, merge_props)

        expected_query = """UNWIND $props AS properties
MERGE (n:Foo:Bar { sid: properties.sid, other: properties.other } )
ON CREATE SET n = properties
ON MATCH SET n += properties"""

        assert query == expected_query

    def test_own_param_name(self):
        test_labels = ['Foo', 'Bar']
        merge_props = ['sid', 'other']

        query = nodes_merge_unwind(test_labels, merge_props, property_parameter='nodes')

        expected_query = """UNWIND $nodes AS properties
MERGE (n:Foo:Bar { sid: properties.sid, other: properties.other } )
ON CREATE SET n = properties
ON MATCH SET n += properties"""

        assert query == expected_query

    def test_nodes_are_created(self, graph, clear_graph):
        query = nodes_merge_unwind(['Foo'], ['testid'])

        graph.run(query, props=[{'testid': 1, 'key': 'newvalue'}])

        results = list(graph.run("MATCH (n:Foo) RETURN n"))

        first_row = results[0]
        first_row_first_element = first_row[0]

        assert len(results) == 1
        assert first_row_first_element['testid'] == 1
        assert first_row_first_element['key'] == 'newvalue'

    def test_nodes_are_merged(self, graph, clear_graph):
        graph.run("CREATE (n:Foo) SET n.testid = 1, n.key = 'value', n.other = 'other_value'")

        query = nodes_merge_unwind(['Foo'], ['testid'])

        graph.run(query, props=[{'testid': 1, 'key': 'newvalue'}])

        results = list(graph.run("MATCH (n:Foo) RETURN n"))

        first_row = results[0]
        first_row_first_element = first_row[0]

        assert len(results) == 1
        assert first_row_first_element['testid'] == 1
        assert first_row_first_element['key'] == 'newvalue'
        # assert other value did not change
        assert first_row_first_element['other'] == 'other_value'
