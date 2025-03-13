import pytest
from typing import List

from graphio import NodeModel, RelationshipModel, Relationship, Graph
from graphio.helper import run_query_return_results


@pytest.fixture
def Person():
    class Person(NodeModel):
        labels = ['Person']
        merge_keys = ['name']

        friends = Relationship('MyNode', 'FRIENDS', 'MyNode')

    return Person


class TestRegistryMeta:
    def test_registry_meta(self):
        class MyNode(NodeModel):
            labels = ['Person']
            merge_keys = ['name']

        assert type(NodeModel.get_class_by_name('MyNode')) == type(MyNode)


class TestNodeModel:

    def test_instantiate_node_model(self):
        class MyNode(NodeModel):
            labels: List[str] = ['Person']
            merge_keys: List[str] = ['name']

        node_model = MyNode({'name': 'John'})
        assert isinstance(node_model, NodeModel)
        assert node_model.labels == ['Person']
        assert node_model.merge_keys == ['name']

    def test_create_from_node_model_without_relationships(self, graph, clear_graph):
        class MyNode(NodeModel):
            labels: List[str] = ['Person']
            merge_keys: List[str] = ['name']

        node_model = MyNode({'name': 'John'})
        node_model.create_node(graph)

        result = run_query_return_results(graph, 'MATCH (n:Person) RETURN n')
        assert result[0][0]['name'] == 'John'

    def test_merge_from_node_model_without_relationships(self, graph, clear_graph):
        class MyNode(NodeModel):
            labels: List[str] = ['Person']
            merge_keys: List[str] = ['name']

        node_model = MyNode({'name': 'John'})
        node_model.merge_node(graph)

        result = run_query_return_results(graph, 'MATCH (n:Person) RETURN n')
        assert result[0][0]['name'] == 'John'

        node_model.merge_node(graph)

        result = run_query_return_results(graph, 'MATCH (n:Person) RETURN n')
        assert result[0][0]['name'] == 'John'

        result = run_query_return_results(graph, 'MATCH (n:Person) RETURN count(n)')
        assert result[0][0] == 1

    def test_get_nodeset(self):
        class MyNode(NodeModel):
            labels: List[str] = ['Person']
            merge_keys: List[str] = ['name']

        node_set = MyNode.nodeset()

        assert node_set.labels == ['Person']
        assert node_set.merge_keys == ['name']

    def test_relationship_iterator(self):
        class Person(NodeModel):
            labels = ['Person']
            merge_keys = ['name']

            friends = Relationship('Person', 'FRIENDS', 'Person')

        peter = Person({'name': 'Peter'})
        john = Person({'name': 'John'})
        peter.friends.add(john)

        assert len(peter.relationships) == 1

        for x in peter.relationships:
            assert x.rel_type == 'FRIENDS'
            assert x.source == 'Person'
            assert x.target == 'Person'

    def test_create_node_with_relationship(self, graph, clear_graph):
        class Person(NodeModel):
            labels = ['Person']
            merge_keys = ['name']

            lives_in = Relationship('Person', 'LIVES_IN', 'City')

        class City(NodeModel):
            labels = ['City']
            merge_keys = ['name']

        peter = Person({'name': 'Peter'})
        berlin = City({'name': 'Berlin'})

        peter.lives_in.add(berlin)

        neo = Graph(graph)
        neo.create(peter, berlin)

        result = run_query_return_results(graph, 'MATCH (m:City) RETURN m')
        assert result[0][0]['name'] == 'Berlin'

        result = run_query_return_results(graph, 'MATCH (n:Person)-[r:LIVES_IN]->(m:City) RETURN n, r, m')
        assert result[0][0]['name'] == 'Peter'
        assert result[0][2]['name'] == 'Berlin'

    def test_create_node_with_relationship_chain(self, graph, clear_graph):
        class Person(NodeModel):
            labels = ['Person']
            merge_keys = ['name']

            lives_in = Relationship('Person', 'LIVES_IN', 'City')

        class City(NodeModel):
            labels = ['City']
            merge_keys = ['name']

            located_in = Relationship('City', 'LOCATED_IN', 'Country')

        class Country(NodeModel):
            labels = ['Country']
            merge_keys = ['name']

        peter = Person({'name': 'Peter'})
        berlin = City({'name': 'Berlin'})
        germany = Country({'name': 'Germany'})

        peter.lives_in.add(berlin)
        berlin.located_in.add(germany)

        neo = Graph(graph)
        neo.create(peter, berlin, germany)

        result = run_query_return_results(graph, 'MATCH (m:City) RETURN m')
        assert result[0][0]['name'] == 'Berlin'

        result = run_query_return_results(graph, 'MATCH (n:Person)-[r:LIVES_IN]->(m:City)-[l:LOCATED_IN]->(o:Country) RETURN n, r, m, l, o')
        assert result[0][0]['name'] == 'Peter'
        assert result[0][2]['name'] == 'Berlin'
        assert result[0][4]['name'] == 'Germany'


    def test_merge_node_with_relationship(self, graph, clear_graph):
        class Person(NodeModel):
            labels = ['Person']
            merge_keys = ['name']

            lives_in = Relationship('Person', 'LIVES_IN', 'City')

        class City(NodeModel):
            labels = ['City']
            merge_keys = ['name']

        peter = Person({'name': 'Peter'})
        berlin = City({'name': 'Berlin'})

        peter.lives_in.add(berlin)

        neo = Graph(graph)
        neo.merge(peter, berlin)
        neo.merge(peter, berlin)
        neo.merge(peter, berlin)

        result = run_query_return_results(graph, 'MATCH (m:City) RETURN count(m)')
        assert result[0][0] == 1

        result = run_query_return_results(graph, 'MATCH (n:Person) RETURN count(n)')
        assert result[0][0] == 1

        result = run_query_return_results(graph, 'MATCH (n:Person)-[r:LIVES_IN]->(m:City) RETURN count(r)')
        assert result[0][0] == 1

        result = run_query_return_results(graph, 'MATCH (m:City) RETURN m')
        assert result[0][0]['name'] == 'Berlin'

        result = run_query_return_results(graph, 'MATCH (n:Person)-[r:LIVES_IN]->(m:City) RETURN n, r, m')
        assert result[0][0]['name'] == 'Peter'
        assert result[0][2]['name'] == 'Berlin'


    def test_create_node_with_relationship_and_properties(self, graph, clear_graph):
        class Person(NodeModel):
            labels = ['Person']
            merge_keys = ['name']

            lives_in = Relationship('Person', 'LIVES_IN', 'City')

        class City(NodeModel):
            labels = ['City']
            merge_keys = ['name']

        peter = Person({'name': 'Peter'})
        berlin = City({'name': 'Berlin'})

        peter.lives_in.add(berlin, {'since': 2010, 'why': 'just because'})

        neo = Graph(graph)
        neo.create(peter, berlin)

        result = run_query_return_results(graph, 'MATCH (m:City) RETURN m')
        assert result[0][0]['name'] == 'Berlin'

        result = run_query_return_results(graph, 'MATCH (n:Person)-[r:LIVES_IN]->(m:City) RETURN n, r, m')
        assert result[0][0]['name'] == 'Peter'
        assert result[0][1]['since'] == 2010
        assert result[0][1]['why'] == 'just because'
        assert result[0][2]['name'] == 'Berlin'

    def test_merge_node_with_relationship_and_properties(self, graph, clear_graph):
        class Person(NodeModel):
            labels = ['Person']
            merge_keys = ['name']

            lives_in = Relationship('Person', 'LIVES_IN', 'City')

        class City(NodeModel):
            labels = ['City']
            merge_keys = ['name']

        peter = Person({'name': 'Peter'})
        berlin = City({'name': 'Berlin'})

        peter.lives_in.add(berlin, {'since': 2010, 'why': 'just because'})
        neo = Graph(graph)
        neo.merge(peter, berlin)
        neo.merge(peter, berlin)
        neo.merge(peter, berlin)

        result = run_query_return_results(graph, 'MATCH (m:City) RETURN count(m)')
        assert result[0][0] == 1

        result = run_query_return_results(graph, 'MATCH (n:Person) RETURN count(n)')
        assert result[0][0] == 1

        result = run_query_return_results(graph, 'MATCH (n:Person)-[r:LIVES_IN]->(m:City) RETURN count(r)')
        assert result[0][0] == 1

        result = run_query_return_results(graph, 'MATCH (m:City) RETURN m')
        assert result[0][0]['name'] == 'Berlin'

        result = run_query_return_results(graph, 'MATCH (n:Person)-[r:LIVES_IN]->(m:City) RETURN n, r, m')
        assert result[0][0]['name'] == 'Peter'
        assert result[0][1]['since'] == 2010
        assert result[0][1]['why'] == 'just because'
        assert result[0][2]['name'] == 'Berlin'


    def test_match_dict(self):
        class MyNode(NodeModel):
            labels: List[str] = ['Person']
            merge_keys: List[str] = ['name']

        node_model = MyNode({'name': 'John', 'something': 'other'})
        assert node_model.match_dict == {'name': 'John'}


    def test_relationship_to(self):
        # reset the registry
        NodeModel.registry = []

        class MyNode(NodeModel):
            labels = ['Person']
            merge_keys = ['name']

            friends = Relationship('MyNode', 'FRIENDS', 'MyNode')

        relset = MyNode.friends.dataset()

        assert relset.rel_type == 'FRIENDS'
        assert relset.start_node_labels == ['Person']
        assert relset.end_node_labels == ['Person']
        assert relset.start_node_properties == ['name']
        assert relset.end_node_properties == ['name']


class TestRelationshipModel:

    def test_get_relationshiptset(self):
        class MyNode(NodeModel):
            labels: List[str] = ['Person']
            merge_keys: List[str] = ['name']

        class MyRelationship(RelationshipModel):
            rel_type: str = 'KNOWS'
            source: type[NodeModel] = MyNode
            target: type[NodeModel] = MyNode
            end_node_properties: List[str] = ['name']

        relationship_set = MyRelationship.dataset()

        assert relationship_set.rel_type == 'KNOWS'
        assert relationship_set.start_node_labels == ['Person']
        assert relationship_set.end_node_labels == ['Person']
        assert relationship_set.start_node_properties == ['name']
        assert relationship_set.end_node_properties == ['name']
        assert relationship_set.default_props == None


class TestNodeModelMatch:

    def test_node_match(self, clear_graph, graph):

        class Person(NodeModel):
            labels = ['Person']
            merge_keys = ['name']

        john = Person({'name': 'John'})
        john.merge_node(graph)

        result = run_query_return_results(graph, 'MATCH (m:Person) RETURN m')
        assert result[0][0]['name'] == 'John'

        result = Person.match({'name': 'John'}, graph)

        assert all([x.properties['name'] == 'John' for x in result])
        assert all(isinstance(x, Person) for x in result)

    def test_node_match_multiple_properties(self, clear_graph, graph):

        class Person(NodeModel):
            labels = ['Person']
            merge_keys = ['name']

        p = Person({'name': 'John', 'age': 30})
        p2 = Person({'name': 'Peter', 'age': 40})
        p.merge_node(graph)
        p2.merge_node(graph)

        result = Person.match({'name': 'John', 'age': 30}, graph)

        assert all([x.properties['name'] == 'John' for x in result])
        assert all([x.properties['age'] == 30 for x in result])
        assert all(isinstance(x, Person) for x in result)

    def test_node_match_no_data(self, clear_graph, graph):

        class Person(NodeModel):
            labels = ['Person']
            merge_keys = ['name']

        result = Person.match({'name': 'John'}, graph)

        assert result == []
