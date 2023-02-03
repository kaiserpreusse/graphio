from graphio.queries import nodes_create_unwind, nodes_merge_unwind, nodes_merge_unwind_preserve, nodes_merge_unwind_array_props, \
    nodes_merge_unwind_preserve_array_props, rels_create_unwind, rels_merge_unwind, CypherQuery, get_label_string_from_list_of_labels, \
    match_clause_with_properties, merge_clause_with_properties, match_properties_as_string


def test_match_clause_with_properties():
    assert match_clause_with_properties(['Person', 'Foo'], ['name'], node_variable='n') == 'MATCH (n:Person:Foo { name: properties.name } )'
    assert match_clause_with_properties(['Person', 'Foo'], ['name'], node_variable='m') == 'MATCH (m:Person:Foo { name: properties.name } )'
    assert match_clause_with_properties(['Person', 'Foo'], ['name'], prop_name='$props', node_variable='n') == 'MATCH (n:Person:Foo { name: $props.name } )'


def test_merge_clause_with_properties():
    assert merge_clause_with_properties(['Person', 'Foo'], ['name'], node_variable='n') == 'MERGE (n:Person:Foo { name: properties.name } )'
    assert merge_clause_with_properties(['Person', 'Foo'], ['name'], node_variable='m') == 'MERGE (m:Person:Foo { name: properties.name } )'
    assert merge_clause_with_properties(['Person', 'Foo'], ['name'], prop_name='$props', node_variable='n') == 'MERGE (n:Person:Foo { name: $props.name } )'


def test_match_properties_as_string():
    assert match_properties_as_string(['name', 'age'], 'properties') == 'name: properties.name, age: properties.age'
    assert match_properties_as_string(['name', 'age'], '$props') == 'name: $props.name, age: $props.age'


def test_cypher_query():
    c = CypherQuery('a', 'b')

    assert c.query() == 'a\nb'


def test_get_label_string_from_list_of_labels():
    assert get_label_string_from_list_of_labels(['a', 'b']) == ':a:b'
    assert get_label_string_from_list_of_labels(['a']) == ':a'
    assert get_label_string_from_list_of_labels([]) == ''
    assert get_label_string_from_list_of_labels(None) == ''


class TestNodesCreateUnwind:

    def test_nodes_create_unwind(self):
        q = nodes_create_unwind(['Person'])
        assert q == """UNWIND $props AS properties
CREATE (n:Person)
SET n = properties"""


class TestNodesMergeUnwind:

    def test_nodes_merge_unwind(self):
        q = nodes_merge_unwind(['Person'], ['name'])

        assert q == """UNWIND $props AS properties
MERGE (n:Person { name: properties.name } )
ON CREATE SET n = properties
ON MATCH SET n += properties"""

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


class TestRelsCreate:

    def test_rels_create(self):
        q = rels_create_unwind(['Person'], ['Movie'], ['name'], ['title'], "LIKES")
        assert q == """UNWIND $rels AS rel
MATCH (a:Person), (b:Movie)
WHERE a.name = rel.start_name AND b.title = rel.end_title
CREATE (a)-[r:LIKES]->(b)
SET r = rel.properties RETURN count(r)"""


class TestRelsMerge:

    def test_rels_merge_unwind(self):
        q = rels_merge_unwind(['Person'], ['Movie'], ['name'], ['title'], "LIKES")
        assert q == """UNWIND $rels AS rel
MATCH (a:Person), (b:Movie)
WHERE a.name = rel.start_name AND b.title = rel.end_title
MERGE (a)-[r:LIKES]->(b)
SET r = rel.properties RETURN count(r)"""