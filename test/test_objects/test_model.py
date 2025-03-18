import pytest
import random

from graphio.helper import run_query_return_results
from graphio import Relationship, NodeSet, RelationshipSet


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


class TestNodeModelToDatasets:

    def test_nodeset_from_nodemodel(self, test_base):
        """
        Test if we can add data to a nodeset from a nodemodel
        """

        class Person(test_base.NodeModel):
            name: str
            age: int

            _labels = ['Person']
            _merge_keys = ['name']

        persons = Person.nodeset()

        assert isinstance(persons, NodeSet)

        for i in range(10):
            persons.add_node({'name': f'Person {i}', 'age': i})

        assert len(persons.nodes) == 10

    def test_relationshipset_from_nodemodel(self, test_base):
        """
        Test if we can add data to a relationshipset from a nodemodel
        """

        class Person(test_base.NodeModel):
            name: str
            age: int

            _labels = ['Person']
            _merge_keys = ['name']

            friends: Relationship = Relationship('Person', 'FRIENDS', 'Person')

        friends = Person.friends.dataset()
        assert isinstance(friends, RelationshipSet)

        for i in range(10):
            friends.add_relationship({'name': f'Person {i}', 'age': i}, {'name': f'Person {i - 1}', 'age': i - 1},
                                     {'since': i})

        assert len(friends.relationships) == 10


class TestNodeModel:

    def test_unique_id_dict_basic(self, test_base):
        """Test that _unique_id_dict correctly returns dictionary of merge keys and values"""

        class User(test_base.NodeModel):
            _labels = ["User"]
            _merge_keys = ["username"]
            username: str
            age: int = None

        user = User(username="alice", age=30)

        # Test that _unique_id_dict returns the correct dictionary
        assert user._unique_id_dict == {"username": "alice"}
        assert "age" not in user._unique_id_dict

    def test_unique_id_dict_with_inheritance(self, test_base):
        """Test that _unique_id_dict works with class inheritance"""

        class Person(test_base.NodeModel):
            _labels = ["Person"]
            _merge_keys = ["id"]
            id: str
            name: str = None

        class Employee(Person):
            _labels = ["Person", "Employee"]
            _merge_keys = ["id", "employee_id"]  # Extending merge keys
            employee_id: str
            department: str = None

        employee = Employee(id="p123", employee_id="e456", name="Bob", department="Engineering")

        # Test that inherited merge keys are correctly included
        assert employee._unique_id_dict == {"id": "p123", "employee_id": "e456"}
        assert "name" not in employee._unique_id_dict
        assert "department" not in employee._unique_id_dict

    def test_merge_keys_validation(self, test_base):
        with pytest.raises(ValueError, match="Merge key 'invalid_key' is not a valid model field."):
            class InvalidNodeModel(test_base.NodeModel):
                name: str
                age: int
                _merge_keys = ['invalid_key']

            InvalidNodeModel(name="example", age=30)

    def test_match_dict_on_class(self, test_base):
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

    def test_create_node_with_relationship_chain(self, graph, test_base):
        class Person(test_base.NodeModel):
            name: str
            _labels = ['Person']
            _merge_keys = ['name']

            lives_in: Relationship = Relationship('Person', 'LIVES_IN', 'City')

        class City(test_base.NodeModel):
            name: str
            _labels = ['City']
            _merge_keys = ['name']

            located_in: Relationship = Relationship('City', 'LOCATED_IN', 'Country')

        class Country(test_base.NodeModel):
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

    def test_merge_node_with_relationship(self, graph, test_base):
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

    def test_node_delete(self, graph, test_base):
        class Person(test_base.NodeModel):
            name: str

            _labels = ['Person']
            _merge_keys = ['name']

        john = Person(name='John')
        john.create()

        result = run_query_return_results(graph, 'MATCH (m:Person) RETURN m')
        assert result[0][0]['name'] == 'John'

        john.delete()

        result = run_query_return_results(graph, 'MATCH (m:Person) RETURN m')
        assert result == []


class TestRelationshipOnNodeModel:

    def test_relationship_on_instance(self, test_base):
        class Person(test_base.NodeModel):
            name: str
            _labels = ['Person']
            _merge_keys = ['name']

            friends: Relationship = Relationship('Person', 'FRIENDS', 'Person')

        john = Person(name='John')
        peter = Person(name='Peter')

        john.friends.add(peter)

        assert len(john.friends) == 1

    def test_many_to_many_relationships(self, test_base, graph):
        """
        .merge() on a node merges source node, targer nodes, and then relationships.

        In the beginning there was an issue that too many relationships were created.
        """

        class Person(test_base.NodeModel):
            name: str
            _labels = ['Person']
            _merge_keys = ['name']

            lives_in: Relationship = Relationship('Person', 'FRIENDS', 'City')

        class City(test_base.NodeModel):
            name: str
            _labels = ['City']
            _merge_keys = ['name']

        # create a few cities
        city_names = ['Berlin', 'Hamburg', 'Munich', 'Minden']
        cities = []
        for city_name in city_names:
            city = City(name=city_name)
            cities.append(city)

        # now create a few Person nodes with random city
        # but only 1 city per person
        for i in range(25):
            person = Person(name=f'Person {i}')
            person.lives_in.add(random.choice(cities))
            person.merge()

        # now assert that we have 25 persons and 4 cities
        # and exactly 25 relationships
        result = run_query_return_results(graph, 'MATCH (m:Person) RETURN m')

        assert len(result) == 25
        result = run_query_return_results(graph, 'MATCH (n:City) RETURN n')
        assert len(result) == 4
        result = run_query_return_results(graph, 'MATCH ()-[r:FRIENDS]->() RETURN r')
        assert len(result) == 25


class TestNodeModelMatch:

    def test_node_match_without_properties(self, test_base, graph):
        class Person(test_base.NodeModel):
            name: str
            _labels = ['Person']
            _merge_keys = ['name']

        john = Person(name='John')
        john.merge()
        peter = Person(name='Peter')
        peter.merge()

        result = run_query_return_results(graph, 'MATCH (m:Person) RETURN m')

        assert result[0][0]['name'] == 'John'
        assert result[1][0]['name'] == 'Peter'

        result = Person.match()
        assert len(result) == 2
        assert all(isinstance(x, Person) for x in result)

    def test_node_match(self, test_base, graph):
        class Person(test_base.NodeModel):
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

    def test_node_match_multiple_properties(self, test_base, graph):
        class Person(test_base.NodeModel):
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

    def test_node_match_no_data(self, test_base, graph):
        class Person(test_base.NodeModel):
            name: str
            age: int

            _labels = ['Person']
            _merge_keys = ['name']

        result = Person.match(name='John')

        assert result == []

    def test_node_match_with_addtional_properties(self, test_base):
        class Person(test_base.NodeModel):
            name: str

            _labels = ['Person']
            _merge_keys = ['name']

        john = Person(name='John', age=30)
        john.merge()

        result = Person.match(name='John', age=30)

        assert all([x.name == 'John' for x in result])
        assert all([x.age == 30 for x in result])
        assert all(isinstance(x, Person) for x in result)

    def test_matching_relationships(self, graph, test_base):
        class Person(test_base.NodeModel):
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
