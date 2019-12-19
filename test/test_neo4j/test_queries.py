from graphio.neo4j.queries import nodes_create_unwind


def test_nodes_create_unwind_single_label():
    test_labels = ['Foo']
    query = nodes_create_unwind(test_labels)
    assert query == 'UNWIND $props AS properties CREATE (n:Foo) SET n = properties'


def test_nodes_create_unwind_mulitple_labels():
    test_labels = ['Foo', 'Bar']
    query = nodes_create_unwind(test_labels)
    assert query == 'UNWIND $props AS properties CREATE (n:Foo:Bar) SET n = properties'


