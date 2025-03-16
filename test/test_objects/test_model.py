import pytest

from graphio.helper import run_query_return_results
from graphio import Relationship


class TestRegistryMeta:
    def test_registry_meta(self, test_base):
        # Define a class using the Base from the test_base fixture
        class MyNode(test_base.NodeModel):
            name: str

            _labels = ['Person']
            _merge_keys = ['name']

        # Explicitly register the class with the registry
        test_base.get_registry().add(MyNode)

        # Test class registration and lookup
        retrieved_class = test_base.get_class_by_name('MyNode')

        # Print registry contents for debugging
        print(test_base.get_registry().default)

        # Assertions
        assert retrieved_class is not None
        assert retrieved_class == MyNode


class TestCreateIndex:
    def test_create_index(self, graph, test_base):
        class TestNode(test_base.NodeModel):
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

            if row['labelsOrTypes'] == TestNode._labels:
                found_label = True
            if row['properties'] == TestNode._merge_keys:
                found_property = True

        assert found_label
        assert found_property


class TestNodeModel:

    def test_merge_keys_validation(self, test_base):
        with pytest.raises(ValueError, match="Merge key 'invalid_key' is not a valid model field."):
            class InvalidNodeModel(test_base.NodeModel):
                name: str
                age: int
                _merge_keys = ['invalid_key']

            InvalidNodeModel(name="example", age=30)

    def test_match_dict(self, test_base):
        class MyNode(test_base.NodeModel):
            name: str
            something: str
            _labels = ['Person']
            _merge_keys = ['name']

        node_model = MyNode(name='John', something='other')
        assert node_model.match_dict == {'name': 'John'}

    def test_model_create(self, test_base, graph):
        class TestNode(test_base.NodeModel):
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

    def test_model_create_with_additional_properties(self, test_base, graph):
        class TestNode(test_base.NodeModel):
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

    def test_merge_node(self, test_base, graph):
        class TestNode(test_base.NodeModel):
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

    def test_relationship_iterator(self, test_base):
        class Person(test_base.NodeModel):
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

    def test_create_node_with_relationship(self, graph, test_base):
        class Person(test_base.NodeModel):
            name: str

            _labels = ['Person']
            _merge_keys = ['name']

            lives_in: Relationship = Relationship('Person', 'LIVES_IN', 'City')

        class City(test_base.NodeModel):
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

        # create again to test if the nodes and relationships are created again
        peter.create()
        result = run_query_return_results(graph, 'MATCH (m:City) RETURN m')

        assert len(result) == 2
        assert result[0][0]['name'] == 'Berlin'
        assert result[1][0]['name'] == 'Berlin'

        result = run_query_return_results(graph, 'MATCH (m:Person) RETURN m')

        assert len(result) == 2
        assert result[0][0]['name'] == 'Peter'
        assert result[1][0]['name'] == 'Peter'

        result = run_query_return_results(graph, 'MATCH (n:Person)-[r:LIVES_IN]->(m:City) RETURN n, r, m')

        # we expect 5 relationships because the nodes were created once with one relationship
        # in the second run the source and target node were created again and 4
        # relationships between two source nodes and two target nodes were created
        assert len(result) == 5
        for i in range(5):
            assert result[i][0]['name'] == 'Peter'
            assert result[i][2]['name'] == 'Berlin'
            assert result[i][0]['name'] == 'Peter'
            assert result[i][2]['name'] == 'Berlin'


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

        result = run_query_return_results(graph, 'MATCH (m:City) RETURN m')
        assert result[0][0]['name'] == 'Berlin'

        result = run_query_return_results(graph,
                                          'MATCH (n:Person)-[r:LIVES_IN]->(m:City)-[l:LOCATED_IN]->(o:Country) RETURN n, r, m, l, o')
        assert result[0][0]['name'] == 'Peter'
        assert result[0][2]['name'] == 'Berlin'
        assert result[0][4]['name'] == 'Germany'

    def test_merge_node_with_relationship(self, graph, clear_graph):
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

        peter.merge()
        peter.merge()
        peter.merge()

        result = run_query_return_results(graph, 'MATCH (m:City) RETURN m')
        assert len(result) == 1
        assert result[0][0]['name'] == 'Berlin'

        result = run_query_return_results(graph, 'MATCH (n:Person)-[r:LIVES_IN]->(m:City) RETURN n, r, m')
        assert len(result) == 1
        assert result[0][0]['name'] == 'Peter'
        assert result[0][2]['name'] == 'Berlin'


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

    def test_matching_relationships(self, graph, clear_graph):
        class Person(_NodeModel):
            name: str

            _labels = ['Person']
            _merge_keys = ['name']

            friends: Relationship = Relationship('Person', 'FRIENDS', 'Person')

        john = Person(name='John')
        peter = Person(name='Peter')
        bob = Person(name='Bob')

        john.friends.add(peter)
        john.friends.add(bob)

        john.create()

        johns_friends = john.friends.match()

        assert len(johns_friends) == 2
        assert all(isinstance(x, Person) for x in johns_friends)
        assert set([x.name for x in johns_friends]) == {'Peter', 'Bob'}
