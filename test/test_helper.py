from graphio.helper import create_single_index, create_composite_index
from graphio.graph import run_query_return_results

def test_create_single_index(graph, clear_graph):
    test_label = 'Foo'
    test_prop = 'bar'

    create_single_index(graph, test_label, test_prop)

    # TODO keep until 4.2 is not supported anymore
    try:
        result = run_query_return_results(graph, "SHOW INDEXES YIELD *")
    except:
        result = run_query_return_results(graph, "CALL db.indexes()")
    row = result[0]

    # the result of the db.indexes() procedure is different for Neo4j 3.5 and 4
    # this should also be synced with differences in py2neo versions
    if 'tokenNames' in row:
        assert row['tokenNames'] == [test_label]
        assert row['properties'] == [test_prop]

    elif 'labelsOrTypes' in row:
        assert row['labelsOrTypes'] == [test_label]
        assert row['properties'] == [test_prop]


def test_create_composite_index(graph, clear_graph):
    test_label = 'Foo'
    test_properties = ['bar', 'keks']

    create_composite_index(graph, test_label, test_properties)

    # TODO keep until 4.2 is not supported anymore
    try:
        result = run_query_return_results(graph, "SHOW INDEXES YIELD *")
    except:
        result = run_query_return_results(graph, "CALL db.indexes()")

    row = result[0]

    # the result of the db.indexes() procedure is different for Neo4j 3.5 and 4
    # this should also be synced with differences in py2neo versions
    if 'tokenNames' in row:
        assert row['tokenNames'] == [test_label]
        # cast to set in case lists have different order
        assert set(row['properties']) == set(test_properties)

    elif 'labelsOrTypes' in row:
        assert row['labelsOrTypes'] == [test_label]
        # cast to set in case lists have different order
        assert set(row['properties']) == set(test_properties)
