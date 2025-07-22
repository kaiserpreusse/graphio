from graphio.utils import get_label_string_from_list_of_labels
from graphio.ogm.query_utils import where_clause_with_properties
from graphio.bulk.query_utils import CypherQuery, match_clause_with_properties, merge_clause_with_properties, match_properties_as_string
from graphio.bulk.nodeset import nodes_merge_factory, nodes_create_factory
from graphio.bulk.relationshipset import rels_create_factory, rels_merge_factory


def test_match_clause_with_properties():
    assert match_clause_with_properties(['Person', 'Foo'], ['name'],
                                        node_variable='n') == 'MATCH (n:Person:Foo { name: properties.name } )'
    assert match_clause_with_properties(['Person', 'Foo'], ['name'],
                                        node_variable='m') == 'MATCH (m:Person:Foo { name: properties.name } )'
    assert match_clause_with_properties(['Person', 'Foo'], ['name'], prop_name='$props',
                                        node_variable='n') == 'MATCH (n:Person:Foo { name: $props.name } )'


def test_merge_clause_with_properties():
    assert merge_clause_with_properties(['Person', 'Foo'], ['name'],
                                        node_variable='n') == 'MERGE (n:Person:Foo { name: properties.name } )'
    assert merge_clause_with_properties(['Person', 'Foo'], ['name'],
                                        node_variable='m') == 'MERGE (m:Person:Foo { name: properties.name } )'
    assert merge_clause_with_properties(['Person', 'Foo'], ['name'], prop_name='$props',
                                        node_variable='n') == 'MERGE (n:Person:Foo { name: $props.name } )'


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


class TestNodesCreateFactory:

    def test_nodes_create_unwind(self):
        q = nodes_create_factory(['Person'])
        assert q == """UNWIND $props AS properties
CREATE (n:Person)
SET n = properties"""

    def test_nodes_create_additional_labels(self):
        q = nodes_create_factory(['Person'], additional_labels=['Foo'])
        assert q == """UNWIND $props AS properties
CREATE (n:Person:Foo)
SET n = properties"""



class TestNodesMergeFactory:

    def test_nodes_merge_factory(self):
        q = nodes_merge_factory(['Person'], ['name'])
        assert q == """UNWIND $props AS properties
MERGE (n:Person { name: properties.name } )
ON CREATE SET n = properties
ON MATCH SET n += properties"""

    def test_nodes_merge_factory_preserve(self):
        q = nodes_merge_factory(['Person'], ['name'], preserve=['foo'])
        assert q == """UNWIND $props AS properties
MERGE (n:Person { name: properties.name } )
ON CREATE SET n = properties
ON MATCH SET n += apoc.map.removeKeys(properties, $preserve)"""

    def test_nodes_merge_factory_array_props(self):
        q = nodes_merge_factory(['Person'], ['name'], array_props=['foo', 'bar'])
        assert q == """UNWIND $props AS properties
MERGE (n:Person { name: properties.name } )
ON CREATE SET n = apoc.map.removeKeys(properties, $append_props)
ON CREATE SET n.foo = [properties.foo], n.bar = [properties.bar]
ON MATCH SET n += apoc.map.removeKeys(properties, $append_props)
ON MATCH SET n.foo = n.foo + properties.foo, n.bar = n.bar + properties.bar"""

    def test_nodes_merge_factory_preserve_array_props(self):
        q = nodes_merge_factory(['Person'], ['name'], array_props=['foo', 'bar'], preserve=['bar'])
        assert q == """UNWIND $props AS properties
MERGE (n:Person { name: properties.name } )
ON CREATE SET n = apoc.map.removeKeys(properties, $append_props)
ON CREATE SET n.foo = [properties.foo], n.bar = [properties.bar]
ON MATCH SET n += apoc.map.removeKeys(apoc.map.removeKeys(properties, $append_props), $preserve)
ON MATCH SET n.foo = n.foo + properties.foo"""


class TestRelationshipsCreateFactory:

    def test_rels_create(self):
        q = rels_create_factory(['Person'], ['Movie'], ['name'], ['title'], "LIKES")
        assert q == """UNWIND $rels AS rel
MATCH (a:Person), (b:Movie)
WHERE a.name = rel.start_name AND b.title = rel.end_title
CREATE (a)-[r:LIKES]->(b)
SET r = rel.properties"""


class TestRelsMerge:

    def test_rels_merge_unwind(self):
        q = rels_merge_factory(['Person'], ['Movie'], ['name'], ['title'], "LIKES")
        assert q == """UNWIND $rels AS rel
MATCH (a:Person), (b:Movie)
WHERE a.name = rel.start_name AND b.title = rel.end_title
MERGE (a)-[r:LIKES]->(b)
ON CREATE SET r = rel.properties
ON MATCH SET r += rel.properties"""


class TestMatchClauses:
    """
    Test query functions for matching nodes and relationships
    """
    def test_where_clause_with_properties(self):
        properties = {'sid': '1234', 'name': 'John Doe', 'age': 30}
        expected = "n.sid = properties.sid AND n.name = properties.name AND n.age = properties.age"
        assert where_clause_with_properties(properties) == expected

    def test_where_clause_with_properties_custom_prop_name(self):
        properties = {'sid': '1234', 'name': 'John Doe', 'age': 30}
        expected = "n.sid = custom_props.sid AND n.name = custom_props.name AND n.age = custom_props.age"
        assert where_clause_with_properties(properties, prop_name='custom_props') == expected

    def test_where_clause_with_properties_custom_node_variable(self):
        properties = {'sid': '1234', 'name': 'John Doe', 'age': 30}
        expected = "node.sid = properties.sid AND node.name = properties.name AND node.age = properties.age"
        assert where_clause_with_properties(properties, node_variable='node') == expected

    def test_where_clause_with_properties_custom_prop_name_and_node_variable(self):
        properties = {'sid': '1234', 'name': 'John Doe', 'age': 30}
        expected = "node.sid = custom_props.sid AND node.name = custom_props.name AND node.age = custom_props.age"
        assert where_clause_with_properties(properties, prop_name='custom_props', node_variable='node') == expected