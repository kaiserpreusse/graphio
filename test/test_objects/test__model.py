import pytest
from typing import List

from graphio.objects._model import _NodeModel, GraphModel, Relationship
from graphio.helper import run_query_return_results


class TestRegistryMeta:
    def test_registry_meta(self):
        class MyNode(_NodeModel):
            _labels = ['Person']
            _merge_keys = ['name']

        assert type(_NodeModel.get_class_by_name('MyNode')) == type(MyNode)


class TestCreateIndex:
    def test_create_index(self, graph, clear_graph):
        class TestNode(_NodeModel):
            name: str
            age: int

            _labels = ['TestNode']
            _merge_keys = ['name']

        TestNode.create_index()

        result = run_query_return_results(graph, "SHOW INDEXES YIELD *")

        found_label = False
        found_property = False

        for row in result:
            # the result of the db.indexes() procedure is different for Neo4j 3.5 and 4
            # this should also be synced with differences in py2neo versions
            row = row.data()

            if row['labelsOrTypes'] == TestNode._labels.default:
                found_label = True
            if row['properties'] == TestNode._merge_keys.default:
                found_property = True

        assert found_label
        assert found_property


class TestNodeModel:

    def test_merge_keys_validation(self):
        with pytest.raises(ValueError, match="Merge key 'invalid_key' is not a valid model field."):
            class InvalidNodeModel(_NodeModel):
                name: str
                age: int
                _merge_keys = ['invalid_key']

            InvalidNodeModel(name="example", age=30)

    def test_match_dict(self):
        class MyNode(_NodeModel):
            name: str
            something: str
            _labels: List[str] = ['Person']
            _merge_keys: List[str] = ['name']

        node_model = MyNode(name='John', something='other')
        assert node_model.match_dict == {'name': 'John'}

    def test_model_create(self, graph, clear_graph):
        class TestNode(_NodeModel):
            id: str
            name: str

            _labels = ['TestNode']
            _merge_keys = ['id']

        t = TestNode(id='test', name='test')

        t.create()

        query = "MATCH (n:TestNode) RETURN n"
        results = run_query_return_results(graph, query)

        assert len(results) == 1
        assert results[0]['n']['id'] == 'test'

        t.create()

        query = "MATCH (n:TestNode) RETURN n"
        results = run_query_return_results(graph, query)

        assert len(results) == 2
        assert results[0]['n']['id'] == 'test'
        assert results[1]['n']['id'] == 'test'

    def test_model_create_with_additional_properties(self, graph, clear_graph):
        class TestNode(_NodeModel):
            id: str
            name: str

            _labels = ['TestNode']
            _merge_keys = ['id']

        t = TestNode(id='test', name='test', foo='bar')
        t.create()

        query = "MATCH (n:TestNode) RETURN n"
        results = run_query_return_results(graph, query)

        assert len(results) == 1
        assert results[0]['n']['id'] == 'test'
        assert results[0]['n']['foo'] == 'bar'

    def test_merge_node(self, graph, clear_graph):
        class TestNode(_NodeModel):
            id: str
            name: str

            _labels = ['TestNode']
            _merge_keys = ['id']

        t = TestNode(id='test', name='test')

        t.merge()

        query = "MATCH (n:TestNode) RETURN n"
        results = run_query_return_results(graph, query)

        assert len(results) == 1
        assert results[0]['n']['id'] == 'test'

        t.merge()

        query = "MATCH (n:TestNode) RETURN n"
        results = run_query_return_results(graph, query)

        assert len(results) == 1
        assert results[0]['n']['id'] == 'test'

    def test_relationship_iterator(self):
        class Person(_NodeModel):
            name: str
            age: int

            _labels = ['Person']
            _merge_keys = ['name']

            friends: Relationship = Relationship('Person', 'FRIENDS', 'Person')

        peter = Person(name='Peter', age=30)
        john = Person(name='John', age=40)
        peter.friends.add(john)

        assert len(peter.relationships) == 1

        for x in peter.relationships:
            assert x.rel_type == 'FRIENDS'
            assert x.source == 'Person'
            assert x.target == 'Person'

    def test_create_node_with_relationship(self, graph, clear_graph):
        class Person(_NodeModel):
            name: str

            _labels = ['Person']
            _merge_keys = ['name']

            lives_in: Relationship = Relationship('Person', 'LIVES_IN', 'City')

        class City(_NodeModel):
            name: str

            _labels = ['City']
            _merge_keys = ['name']

        peter = Person(name='Peter')
        berlin = City(name='Berlin')

        peter.lives_in.add(berlin)

        peter.create()

        result = run_query_return_results(graph, 'MATCH (m:City) RETURN m')

        assert result[0][0]['name'] == 'Berlin'

        result = run_query_return_results(graph, 'MATCH (n:Person)-[r:LIVES_IN]->(m:City) RETURN n, r, m')
        assert result[0][0]['name'] == 'Peter'
        assert result[0][2]['name'] == 'Berlin'

    def test_create_node_with_relationship_chain(self, graph, clear_graph):
        class Person(_NodeModel):
            name: str
            _labels = ['Person']
            _merge_keys = ['name']

            lives_in: Relationship = Relationship('Person', 'LIVES_IN', 'City')

        class City(_NodeModel):
            name: str
            _labels = ['City']
            _merge_keys = ['name']

            located_in: Relationship = Relationship('City', 'LOCATED_IN', 'Country')

        class Country(_NodeModel):
            name: str
            _labels = ['Country']
            _merge_keys = ['name']

        peter = Person(name='Peter')
        berlin = City(name='Berlin')
        germany = Country(name='Germany')

        peter.lives_in.add(berlin)
        berlin.located_in.add(germany)

        peter.create()

        # neo = Graph(graph)
        # neo.create(peter, berlin, germany)

        result = run_query_return_results(graph, 'MATCH (m:City) RETURN m')
        assert result[0][0]['name'] == 'Berlin'

        result = run_query_return_results(graph,
                                          'MATCH (n:Person)-[r:LIVES_IN]->(m:City)-[l:LOCATED_IN]->(o:Country) RETURN n, r, m, l, o')
        assert result[0][0]['name'] == 'Peter'
        assert result[0][2]['name'] == 'Berlin'
        assert result[0][4]['name'] == 'Germany'


class TestNodeModelMatch:

    def test_node_match(self, clear_graph, graph):
        class Person(_NodeModel):
            name: str
            _labels = ['Person']
            _merge_keys = ['name']

        john = Person(name='John')
        john.merge()

        result = run_query_return_results(graph, 'MATCH (m:Person) RETURN m')
        assert result[0][0]['name'] == 'John'

        result = Person.match(name='John')

        assert all([x.name == 'John' for x in result])
        assert all(isinstance(x, Person) for x in result)

    def test_node_match_multiple_properties(self, clear_graph, graph):
        class Person(_NodeModel):
            name: str
            age: int

            _labels = ['Person']
            _merge_keys = ['name']

        john = Person(name='John', age=30)
        john.merge()

        peter = Person(name='Peter', age=40)
        peter.merge()

        result = Person.match(name='John', age=30)

        assert all([x.name == 'John' for x in result])
        assert all([x.age == 30 for x in result])
        assert all(isinstance(x, Person) for x in result)

    def test_node_match_no_data(self, clear_graph, graph):
        class Person(_NodeModel):
            name: str
            age: int

            _labels = ['Person']
            _merge_keys = ['name']

        result = Person.match(name='John')

        assert result == []

    def test_node_match_with_addtional_properties(self):
        class Person(_NodeModel):
            name: str

            _labels = ['Person']
            _merge_keys = ['name']

        john = Person(name='John', age=30)
        john.merge()

        result = Person.match(name='John', age=30)

        assert all([x.name == 'John' for x in result])
        assert all([x.age == 30 for x in result])
        assert all(isinstance(x, Person) for x in result)

class Person(_NodeModel):
    __labels__ = ['Person']
    __merge_keys__ = ['name']

    name: str
    age: int


class Person(_NodeModel):
    _labels = ['Person']
    _merge_keys = ['name']

    name: str
    age: int