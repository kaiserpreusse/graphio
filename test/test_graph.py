from graphio.graph import run_query_return_results

def test_create_query_fixed_property(graph, clear_graph):

    q = "CREATE (a:Test) SET a.key = 'value'"

    run_query_return_results(graph, q)

    r = run_query_return_results(graph, "MATCH (a:Test) RETURN count(a)")
    assert r[0][0] == 1
