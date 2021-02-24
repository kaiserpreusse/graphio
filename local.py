from py2neo import Graph
from py2neo.bulk import create_nodes, create_relationships

graph = Graph(host='localhost', password='test')

# delete DB
graph.run("MATCH (a) DETACH DELETE a")

# create some nodes: (:Source) and (:Target)
node_data = [{'uid': 1}, {'uid': 2}]

create_nodes(graph, node_data, labels=['Source'])
create_nodes(graph, node_data, labels=['Target'])

# create relationships with single property
rel_data = [
    (1, {}, 1),
    (2, {}, 2)
]

create_relationships(graph, rel_data, 'TEST_SINGLE_PROP', start_node_key=('Source', 'uid'), end_node_key=('Target', 'uid'))

# try to create relationships with tuples of length 1
# these relationships are not created
rel_data = [
    ((1,), {}, (1,)),
    ((2,), {}, (2,))
]

create_relationships(graph, rel_data, 'TEST_TUPLE_1', start_node_key=('Source', 'uid'), end_node_key=('Target', 'uid'))

