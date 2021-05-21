from graphio.queries import nodes_merge_unwind_preserve, nodes_merge_unwind_array_props, \
    nodes_merge_unwind_preserve_array_props, CypherQuery


def test_cypher_query():
    c = CypherQuery('a', 'b')

    assert c.query() == 'a\nb'


class TestNodesMergeUnwind:

    def test_nodes_merge_unwind_preserve(self):
        q = nodes_merge_unwind_preserve(['Person'], ['name'])
        print(q)
        assert q == """UNWIND $props AS properties
MERGE (n:Person { name: properties.name } )
ON CREATE SET n = properties
ON MATCH SET n += apoc.map.removeKeys(properties, $preserve)"""

    def test_nodes_merge_unwind_array_props(self):
        q = nodes_merge_unwind_array_props(['Person'], ['name'], ['foo', 'bar'])
        print(q)
        assert q == """UNWIND $props AS properties
MERGE (n:Person { name: properties.name } )
ON CREATE SET n = apoc.map.removeKeys(properties, $append_props)
ON CREATE SET n.foo = [properties.foo], n.bar = [properties.bar]
ON MATCH SET n += apoc.map.removeKeys(properties, $append_props)
ON MATCH SET n.foo = n.foo + properties.foo, n.bar = n.bar + properties.bar"""

    def test_nodes_merge_unwind_preserve_array_props(self):
        q = nodes_merge_unwind_preserve_array_props(['Person'], ['name'], ['foo', 'bar'], ['bar'])
        print(q)
        assert q == """UNWIND $props AS properties
MERGE (n:Person { name: properties.name } )
ON CREATE SET n = apoc.map.removeKeys(properties, $append_props)
ON CREATE SET n.foo = [properties.foo], n.bar = [properties.bar]
ON MATCH SET n += apoc.map.removeKeys(apoc.map.removeKeys(properties, $append_props), $preserve)
ON MATCH SET n.foo = n.foo + properties.foo"""
