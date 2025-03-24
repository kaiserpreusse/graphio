import pytest
import random
from datetime import datetime, timedelta

from graphio.helper import run_query_return_results
from graphio import Relationship, NodeSet, RelationshipSet, FilterOp, CypherQuery, RelField


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


class TestNodeModelCreateMerge:

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


class TestRelationshipCreateMerge:

    def test_relationship_on_instance(self, test_base):
        class Person(test_base.NodeModel):
            name: str
            _labels = ['Person']
            _merge_keys = ['name']

            friends: Relationship = Relationship('Person', 'FRIENDS', 'Person')

        john = Person(name='John')
        peter = Person(name='Peter')

        john.friends.add(peter)

        assert len(john.friends.nodes) == 1

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

    def test_delete_all_relationships(self, graph, test_base):
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

        # assert data is in DB
        result = run_query_return_results(graph, 'MATCH (n:Person)-[r:LIVES_IN]->(m:City) RETURN n, r, m')
        assert len(result) == 1
        assert result[0][0]['name'] == 'Peter'
        assert result[0][2]['name'] == 'Berlin'

        peter.lives_in.delete()
        result = run_query_return_results(graph, "MATCH ()-[r:LIVES_IN]->() RETURN r")
        assert len(result) == 0


    def test_delete_all_relationships_assert_other_relationships_still_exist(self, graph, test_base):
        class Person(test_base.NodeModel):
            name: str

            _labels = ['Person']
            _merge_keys = ['name']

            lives_in: Relationship = Relationship('Person', 'LIVES_IN', 'City')
            friends: Relationship = Relationship('Person', 'FRIENDS', 'Person')

        class City(test_base.NodeModel):
            name: str

            _labels = ['City']
            _merge_keys = ['name']

        peter = Person(name='Peter')
        berlin = City(name='Berlin')
        bob = Person(name='Bob')

        peter.lives_in.add(berlin)
        peter.friends.add(bob)

        peter.create()

        # assert data is in DB
        result = run_query_return_results(graph, 'MATCH (n:Person)-[r:LIVES_IN]->(m:City) RETURN n, r, m')
        assert len(result) == 1
        assert result[0][0]['name'] == 'Peter'
        assert result[0][2]['name'] == 'Berlin'

        result = run_query_return_results(graph, 'MATCH (n:Person)-[r:FRIENDS]->(m:Person) RETURN n, r, m')
        assert len(result) == 1
        assert result[0][0]['name'] == 'Peter'
        assert result[0][2]['name'] == 'Bob'

        peter.lives_in.delete()
        result = run_query_return_results(graph, "MATCH ()-[r:LIVES_IN]->() RETURN r")
        assert len(result) == 0

        result = run_query_return_results(graph, 'MATCH (n:Person)-[r:FRIENDS]->(m:Person) RETURN n, r, m')
        assert len(result) == 1
        assert result[0][0]['name'] == 'Peter'
        assert result[0][2]['name'] == 'Bob'


    def test_delete_specific_relationships(self, graph, test_base):
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
        london = City(name='London')

        peter.lives_in.add(berlin)
        peter.lives_in.add(london)

        peter.create()

        # assert data is in DB
        result = run_query_return_results(graph, 'MATCH (n:Person)-[r:LIVES_IN]->(m:City) RETURN n, r, m ORDER BY m.name ASC')
        assert len(result) == 2
        assert result[0][0]['name'] == 'Peter'
        assert result[0][2]['name'] == 'Berlin'
        assert result[1][0]['name'] == 'Peter'
        assert result[1][2]['name'] == 'London'

        peter.lives_in.delete(london)
        # assert data is in DB
        result = run_query_return_results(graph, 'MATCH (n:Person)-[r:LIVES_IN]->(m:City) RETURN n, r, m')
        assert len(result) == 1
        assert result[0][0]['name'] == 'Peter'
        assert result[0][2]['name'] == 'Berlin'

        peter.lives_in.delete(berlin)
        result = run_query_return_results(graph, "MATCH ()-[r:LIVES_IN]->() RETURN r")
        assert len(result) == 0


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

        result = run_query_return_results(graph, 'MATCH (m:Person) RETURN m ORDER BY m.name ASC')

        assert result[0][0]['name'] == 'John'
        assert result[1][0]['name'] == 'Peter'

        result = Person.match().all()
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

        result = Person.match(name='John').all()

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

        result = Person.match(name='John', age=30).all()

        assert all([x.name == 'John' for x in result])
        assert all([x.age == 30 for x in result])
        assert all(isinstance(x, Person) for x in result)

    def test_node_match_no_data(self, test_base, graph):
        class Person(test_base.NodeModel):
            name: str
            age: int

            _labels = ['Person']
            _merge_keys = ['name']

        result = Person.match(name='John').all()

        assert result == []

    def test_node_match_with_addtional_properties(self, test_base):
        class Person(test_base.NodeModel):
            name: str

            _labels = ['Person']
            _merge_keys = ['name']

        john = Person(name='John', age=30)
        john.merge()

        result = Person.match(name='John', age=30).all()

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

        johns_friends = john.friends.match().all()

        assert len(johns_friends) == 2
        assert all(isinstance(x, Person) for x in johns_friends)
        assert set([x.name for x in johns_friends]) == {'Peter', 'Bob'}


class TestNodeModelMatchFilterOps:
    def test_equality_filtering(self, graph, test_base):
        """Test basic equality filtering using keyword arguments"""

        class Person(test_base.NodeModel):
            name: str
            age: int

            _labels = ['Person']
            _merge_keys = ['name']

        # Create test data
        Person(name='John', age=30).merge()
        Person(name='Peter', age=40).merge()
        Person(name='Sarah', age=30).merge()

        # Test filtering with equality
        result = Person.match(name='John').all()
        assert len(result) == 1
        assert result[0].name == 'John'
        assert result[0].age == 30

        # Test filtering with multiple equality conditions
        result = Person.match(age=30).all()
        assert len(result) == 2
        assert {p.name for p in result} == {'John', 'Sarah'}

    def test_complex_filtering_with_filter_op(self, graph, test_base):
        """Test filtering using FilterOp for complex conditions"""

        class Person(test_base.NodeModel):
            name: str
            age: int

            _labels = ['Person']
            _merge_keys = ['name']

        # Create test data
        Person(name='John', age=30).merge()
        Person(name='Peter', age=40).merge()
        Person(name='Sarah', age=25).merge()

        # Test greater than filter
        result = Person.match(FilterOp("age", ">", 30)).all()
        assert len(result) == 1
        assert result[0].name == 'Peter'

        # Test less than filter
        result = Person.match(FilterOp("age", "<", 30)).all()
        assert len(result) == 1
        assert result[0].name == 'Sarah'

        # Test multiple conditions combined
        result = Person.match(FilterOp("age", ">=", 25), FilterOp("age", "<=", 30)).all()
        assert len(result) == 2
        assert {p.name for p in result} == {'John', 'Sarah'}


class TestNodeModelMatchCypherQuery:
    def test_basic_cypher_query(self, graph, test_base):
        """Test basic Cypher query functionality"""

        class Person(test_base.NodeModel):
            name: str
            age: int

            _labels = ['Person']
            _merge_keys = ['name']

        # Create test data
        Person(name='John', age=30).merge()
        Person(name='Peter', age=40).merge()
        Person(name='Sarah', age=25).merge()

        # Test simple Cypher query
        query = """
        MATCH (n:Person)
        RETURN n
        """
        result = Person.match(CypherQuery(query)).all()
        assert len(result) == 3
        assert {p.name for p in result} == {'John', 'Peter', 'Sarah'}
        assert all(isinstance(p, Person) for p in result)

    def test_cypher_query_with_where_clause(self, graph, test_base):
        """Test Cypher query with WHERE clause"""

        class Person(test_base.NodeModel):
            name: str
            age: int

            _labels = ['Person']
            _merge_keys = ['name']

        # Create test data
        Person(name='John', age=30).merge()
        Person(name='Peter', age=40).merge()
        Person(name='Sarah', age=25).merge()

        # Test Cypher query with WHERE clause
        query = """
        MATCH (n:Person)
        WHERE n.age > 30
        RETURN n
        """
        result = Person.match(CypherQuery(query)).all()
        assert len(result) == 1
        assert result[0].name == 'Peter'
        assert result[0].age == 40

    def test_cypher_query_with_parameters(self, graph, test_base):
        """Test Cypher query with parameters"""

        class Person(test_base.NodeModel):
            name: str
            age: int

            _labels = ['Person']
            _merge_keys = ['name']

        # Create test data
        Person(name='John', age=30).merge()
        Person(name='Peter', age=40).merge()
        Person(name='Sarah', age=25).merge()

        # Test Cypher query with parameters
        query = """
        MATCH (n:Person)
        WHERE n.age > $min_age
        RETURN n
        """
        result = Person.match(CypherQuery(query, params={"min_age": 25})).all()
        assert len(result) == 2
        assert {p.name for p in result} == {'John', 'Peter'}
        assert all(p.age > 25 for p in result)

    def test_cypher_query_with_relationships(self, graph, test_base):
        """Test Cypher query with relationships"""

        class Person(test_base.NodeModel):
            name: str
            age: int

            _labels = ['Person']
            _merge_keys = ['name']

            friends: Relationship = Relationship('Person', 'FRIENDS', 'Person')
            lives_in: Relationship = Relationship('Person', 'LIVES_IN', 'City')

        class City(test_base.NodeModel):
            name: str

            _labels = ['City']
            _merge_keys = ['name']

        # Create test data
        john = Person(name='John', age=30)
        peter = Person(name='Peter', age=40)
        sarah = Person(name='Sarah', age=25)

        london = City(name='London')
        berlin = City(name='Berlin')

        john.friends.add(peter)
        peter.friends.add(sarah)

        john.lives_in.add(london)
        peter.lives_in.add(berlin)
        sarah.lives_in.add(berlin)

        john.merge()
        peter.merge()
        sarah.merge()

        # Test query finding people who live in Berlin
        query = """
        MATCH (n:Person)-[:LIVES_IN]->(:City {name: $city_name})
        RETURN n
        """
        result = Person.match(CypherQuery(query, params={"city_name": "Berlin"})).all()
        assert len(result) == 2
        assert {p.name for p in result} == {'Peter', 'Sarah'}

        # Test query finding friends of friends
        query = """
        MATCH (n:Person)-[:FRIENDS]->()-[:FRIENDS]->()
        RETURN DISTINCT n
        """
        result = Person.match(CypherQuery(query)).all()
        assert len(result) == 1
        assert result[0].name == 'John'

    def test_cypher_query_with_aggregation(self, graph, test_base):
        """Test Cypher query with aggregation"""

        class Person(test_base.NodeModel):
            name: str
            age: int
            city: str

            _labels = ['Person']
            _merge_keys = ['name']

        # Create test data
        Person(name='John', age=30, city='London').merge()
        Person(name='Peter', age=40, city='Berlin').merge()
        Person(name='Sarah', age=25, city='Berlin').merge()
        Person(name='Mike', age=35, city='London').merge()

        # Test query using aggregation to find cities with multiple people over 25
        query = """
        MATCH (n:Person)
        WHERE n.age > $min_age
        WITH n.city as city, count(*) as count
        WHERE count > 1
        MATCH (n:Person)
        WHERE n.city = city
        RETURN n
        """
        result = Person.match(CypherQuery(query, params={"min_age": 25})).all()
        assert len(result) == 2
        assert {p.name for p in result} == {'John', 'Mike'}
        assert all(p.city == 'London' for p in result)

    def test_cypher_query_with_ordering(self, graph, test_base):
        """Test Cypher query with ordering"""

        class Person(test_base.NodeModel):
            name: str
            age: int

            _labels = ['Person']
            _merge_keys = ['name']

        # Create test data
        Person(name='John', age=30).merge()
        Person(name='Peter', age=40).merge()
        Person(name='Sarah', age=25).merge()

        # Test query with ordering
        query = """
        MATCH (n:Person)
        ORDER BY n.age DESC
        RETURN n
        """
        result = Person.match(CypherQuery(query)).all()
        assert len(result) == 3
        assert [p.name for p in result] == ['Peter', 'John', 'Sarah']
        assert [p.age for p in result] == [40, 30, 25]

    def test_cypher_query_with_limit(self, graph, test_base):
        """Test Cypher query with limit"""

        class Person(test_base.NodeModel):
            name: str
            age: int

            _labels = ['Person']
            _merge_keys = ['name']

        # Create test data
        Person(name='John', age=30).merge()
        Person(name='Peter', age=40).merge()
        Person(name='Sarah', age=25).merge()

        # Test query with limit
        query = """
        MATCH (n:Person)
        ORDER BY n.age DESC
        LIMIT 2
        RETURN n
        """
        result = Person.match(CypherQuery(query)).all()
        assert len(result) == 2
        assert [p.name for p in result] == ['Peter', 'John']
        assert [p.age for p in result] == [40, 30]

    def test_cypher_query_precedence_over_filters(self, graph, test_base):
        """Test that CypherQuery takes precedence over other filters"""

        class Person(test_base.NodeModel):
            name: str
            age: int

            _labels = ['Person']
            _merge_keys = ['name']

        # Create test data
        Person(name='John', age=30).merge()
        Person(name='Peter', age=40).merge()
        Person(name='Sarah', age=25).merge()

        # When CypherQuery is provided, other filters should be ignored
        query = """
        MATCH (n:Person)
        WHERE n.age > 30
        RETURN n
        """

        # The additional filter (name='John') should be ignored
        result = Person.match(CypherQuery(query), name='John').all()
        assert len(result) == 1
        assert result[0].name == 'Peter'  # Not 'John'
        assert result[0].age == 40

    def test_invalid_cypher_query_variable(self, graph, test_base):
        """Test that using an invalid variable name raises an error"""

        class Person(test_base.NodeModel):
            name: str
            age: int

            _labels = ['Person']
            _merge_keys = ['name']

        # Create test data
        Person(name='John', age=30).merge()

        # Test query with invalid variable name
        query = """
        MATCH (person:Person)
        RETURN person
        """

        with pytest.raises(ValueError, match="must return nodes with variable name 'n'"):
            Person.match(CypherQuery(query)).all()


class TestNodeModelMatchClassAttributes:
    def test_equality_operator(self, graph, test_base):
        """Test basic equality filtering using field operators"""

        class Person(test_base.NodeModel):
            name: str
            age: int

            _labels = ['Person']
            _merge_keys = ['name']

        # Create test data
        Person(name='John', age=30).merge()
        Person(name='Peter', age=40).merge()

        # Test equality operator
        result = Person.match(Person.name == 'John').all()
        assert len(result) == 1
        assert result[0].name == 'John'

        # Test with multiple results
        more_people = ['Alice', 'Bob']
        for name in more_people:
            Person(name=name, age=30).merge()

        result = Person.match(Person.age == 30).all()
        assert len(result) == 3
        assert set(p.name for p in result) == {'John', 'Alice', 'Bob'}

    def test_comparison_operators(self, graph, test_base):
        """Test comparison operators for numeric fields"""

        class Person(test_base.NodeModel):
            name: str
            age: int

            _labels = ['Person']
            _merge_keys = ['name']

        # Create test data with different ages
        Person(name='Young', age=20).merge()
        Person(name='Middle', age=40).merge()
        Person(name='Old', age=60).merge()

        # Test greater than
        result = Person.match(Person.age > 50).all()
        assert len(result) == 1
        assert result[0].name == 'Old'

        # Test less than
        result = Person.match(Person.age < 30).all()
        assert len(result) == 1
        assert result[0].name == 'Young'

        # Test greater than or equal
        result = Person.match(Person.age >= 40).all()
        assert len(result) == 2
        assert set(p.name for p in result) == {'Middle', 'Old'}

        # Test less than or equal
        result = Person.match(Person.age <= 40).all()
        assert len(result) == 2
        assert set(p.name for p in result) == {'Young', 'Middle'}

    def test_string_operations(self, graph, test_base):
        """Test string operations with field references"""

        class Product(test_base.NodeModel):
            name: str

            _labels = ['Product']
            _merge_keys = ['name']

        # Create test data
        products = [
            'iPhone', 'iPad', 'MacBook',
            'Galaxy Phone', 'Galaxy Tab',
            'ThinkPad'
        ]

        for p in products:
            Product(name=p).merge()

        # Test starts_with
        result = Product.match(Product.name.starts_with('i')).all()
        assert len(result) == 2
        assert set(p.name for p in result) == {'iPhone', 'iPad'}

        # Test ends_with
        result = Product.match(Product.name.ends_with('Pad')).all()
        assert len(result) == 2
        assert set(p.name for p in result) == {'iPad', 'ThinkPad'}

        # Test contains
        result = Product.match(Product.name.contains('Galaxy')).all()
        assert len(result) == 2
        assert set(p.name for p in result) == {'Galaxy Phone', 'Galaxy Tab'}

    def test_multiple_conditions(self, graph, test_base):
        """Test multiple filtering conditions combined"""

        class Person(test_base.NodeModel):
            name: str
            age: int
            city: str

            _labels = ['Person']
            _merge_keys = ['name']

        # Create test data
        data = [
            ('John', 30, 'New York'),
            ('Peter', 40, 'London'),
            ('Sarah', 35, 'New York'),
            ('Mike', 25, 'London'),
            ('Anna', 30, 'Berlin')
        ]

        for name, age, city in data:
            Person(name=name, age=age, city=city).merge()

        # Test combining field operators and methods
        result = Person.match(
            Person.age >= 30,
            Person.city.contains('York')
        ).all()
        assert len(result) == 2
        assert set(p.name for p in result) == {'John', 'Sarah'}

        # Test different combinations
        result = Person.match(
            Person.age < 35,
            Person.name.starts_with('M')
        ).all()
        assert len(result) == 1
        assert result[0].name == 'Mike'

    def test_invalid_field_error(self, test_base):
        """Test that using an invalid field raises an error"""

        class Person(test_base.NodeModel):
            name: str

            _labels = ['Person']
            _merge_keys = ['name']

        # This should raise an error because 'age' is not a field
        with pytest.raises(AttributeError):
            Person.match(Person.age == 30).all()

    def test_date_equality_filtering(self, graph, test_base):
        """Test date equality filtering"""
        from datetime import date

        class Event(test_base.NodeModel):
            name: str
            event_date: date

            _labels = ['Event']
            _merge_keys = ['name']

        # Create test data with different dates
        Event(name='Conference', event_date=date(2023, 6, 15)).merge()
        Event(name='Workshop', event_date=date(2023, 7, 20)).merge()
        Event(name='Meetup', event_date=date(2023, 6, 15)).merge()

        # Test equality operator with dates
        result = Event.match(Event.event_date == date(2023, 6, 15)).all()
        assert len(result) == 2
        assert set(e.name for e in result) == {'Conference', 'Meetup'}

        # Test another date
        result = Event.match(Event.event_date == date(2023, 7, 20)).all()
        assert len(result) == 1
        assert result[0].name == 'Workshop'

    def test_date_comparison_operators(self, graph, test_base):
        """Test comparison operators with dates"""
        from datetime import date

        class Event(test_base.NodeModel):
            name: str
            event_date: date

            _labels = ['Event']
            _merge_keys = ['name']

        # Create test data with different dates
        Event(name='Past', event_date=date(2022, 5, 10)).merge()
        Event(name='Present', event_date=date(2023, 6, 15)).merge()
        Event(name='Future', event_date=date(2024, 7, 20)).merge()

        # Test greater than
        result = Event.match(Event.event_date > date(2023, 1, 1)).all()
        assert len(result) == 2
        assert set(e.name for e in result) == {'Present', 'Future'}

        # Test less than
        result = Event.match(Event.event_date < date(2023, 7, 1)).all()
        assert len(result) == 2
        assert set(e.name for e in result) == {'Past', 'Present'}

        # Test greater than or equal
        result = Event.match(Event.event_date >= date(2023, 6, 15)).all()
        assert len(result) == 2
        assert set(e.name for e in result) == {'Present', 'Future'}

        # Test less than or equal
        result = Event.match(Event.event_date <= date(2022, 5, 10)).all()
        assert len(result) == 1
        assert result[0].name == 'Past'

    def test_datetime_equality_filtering(self, graph, test_base):
        """Test datetime equality filtering"""
        from datetime import datetime

        class LogEntry(test_base.NodeModel):
            id: str
            timestamp: datetime

            _labels = ['LogEntry']
            _merge_keys = ['id']

        # Create test data
        LogEntry(id='log1', timestamp=datetime(2023, 6, 15, 10, 30, 0)).merge()
        LogEntry(id='log2', timestamp=datetime(2023, 6, 15, 14, 45, 0)).merge()
        LogEntry(id='log3', timestamp=datetime(2023, 6, 16, 9, 15, 0)).merge()

        # Test equality
        result = LogEntry.match(LogEntry.timestamp == datetime(2023, 6, 15, 10, 30, 0)).all()
        assert len(result) == 1
        assert result[0].id == 'log1'

        # Test another equality
        result = LogEntry.match(LogEntry.timestamp == datetime(2023, 6, 16, 9, 15, 0)).all()
        assert len(result) == 1
        assert result[0].id == 'log3'

    def test_datetime_comparison_operators(self, graph, test_base):
        """Test comparison operators with datetimes"""
        from datetime import datetime

        class LogEntry(test_base.NodeModel):
            id: str
            timestamp: datetime

            _labels = ['LogEntry']
            _merge_keys = ['id']

        # Create test data
        LogEntry(id='morning', timestamp=datetime(2023, 6, 15, 9, 0, 0)).merge()
        LogEntry(id='noon', timestamp=datetime(2023, 6, 15, 12, 0, 0)).merge()
        LogEntry(id='evening', timestamp=datetime(2023, 6, 15, 18, 0, 0)).merge()

        # Test greater than
        result = LogEntry.match(LogEntry.timestamp > datetime(2023, 6, 15, 11, 0, 0)).all()
        assert len(result) == 2
        assert set(e.id for e in result) == {'noon', 'evening'}

        # Test less than
        result = LogEntry.match(LogEntry.timestamp < datetime(2023, 6, 15, 12, 30, 0)).all()
        assert len(result) == 2
        assert set(e.id for e in result) == {'morning', 'noon'}

        # Test range query
        result = LogEntry.match(
            LogEntry.timestamp >= datetime(2023, 6, 15, 9, 0, 0),
            LogEntry.timestamp <= datetime(2023, 6, 15, 12, 0, 0)
        ).all()
        assert len(result) == 2
        assert set(e.id for e in result) == {'morning', 'noon'}

    def test_date_and_datetime_combined_filtering(self, graph, test_base):
        """Test combining date/datetime with other properties"""
        from datetime import date, datetime

        class Appointment(test_base.NodeModel):
            title: str
            appointment_date: date
            start_time: datetime
            is_confirmed: bool

            _labels = ['Appointment']
            _merge_keys = ['title']

        # Create test data
        Appointment(
            title='Dentist',
            appointment_date=date(2023, 6, 15),
            start_time=datetime(2023, 6, 15, 10, 0, 0),
            is_confirmed=True
        ).merge()

        Appointment(
            title='Doctor',
            appointment_date=date(2023, 6, 15),
            start_time=datetime(2023, 6, 15, 14, 0, 0),
            is_confirmed=True
        ).merge()

        Appointment(
            title='Interview',
            appointment_date=date(2023, 6, 16),
            start_time=datetime(2023, 6, 16, 11, 0, 0),
            is_confirmed=False
        ).merge()

        # Test combining date equality with boolean condition
        result = Appointment.match(
            Appointment.appointment_date == date(2023, 6, 15),
            Appointment.is_confirmed == True
        ).all()
        assert len(result) == 2
        assert set(a.title for a in result) == {'Dentist', 'Doctor'}

        # Test combining date and time ranges
        result = Appointment.match(
            Appointment.appointment_date >= date(2023, 6, 15),
            Appointment.start_time > datetime(2023, 6, 15, 12, 0, 0)
        ).all()
        assert len(result) == 2
        assert set(a.title for a in result) == {'Doctor', 'Interview'}


class TestRelationshipMatch:
    def test_relationship_equality_filtering(self, graph, test_base):
        """Test filtering relationships with equality conditions"""

        class Person(test_base.NodeModel):
            name: str
            age: int
            _labels = ['Person']
            _merge_keys = ['name']

            friends: Relationship = Relationship('Person', 'FRIENDS', 'Person')

        # Create test data
        alice = Person(name='Alice', age=30)
        bob = Person(name='Bob', age=40)
        charlie = Person(name='Charlie', age=50)
        dave = Person(name='Dave', age=60)

        alice.friends.add(bob)
        alice.friends.add(charlie)
        alice.friends.add(dave)

        alice.merge()

        # Test simple equality filtering
        result = alice.friends.match(age=50).all()
        assert len(result) == 1
        assert result[0].name == 'Charlie'

        # Test with multiple matches
        result = alice.friends.match(age=60).all()  # Should get Charlie and Dave
        assert len(result) == 1
        assert result[0].name == 'Dave'

    def test_relationship_field_descriptor_filtering(self, graph, test_base):
        """Test filtering relationships with field descriptors"""

        class Person(test_base.NodeModel):
            name: str
            age: int
            _labels = ['Person']
            _merge_keys = ['name']

            friends: Relationship = Relationship('Person', 'FRIENDS', 'Person')

        # Create test data
        alice = Person(name='Alice', age=30)
        bob = Person(name='Bob', age=40)
        charlie = Person(name='Charlie', age=50)
        dave = Person(name='Dave', age=60)

        alice.friends.add(bob)
        alice.friends.add(charlie)
        alice.friends.add(dave)

        alice.merge()

        # Test with field descriptors
        result = alice.friends.match(Person.age > 45).all()
        assert len(result) == 2
        assert set(p.name for p in result) == {'Charlie', 'Dave'}

        # Test with equality
        result = alice.friends.match(Person.name == 'Bob').all()
        assert len(result) == 1
        assert result[0].name == 'Bob'
        assert result[0].age == 40

    def test_relationship_string_operations(self, graph, test_base):
        """Test string operations on relationship filtering"""

        class Person(test_base.NodeModel):
            name: str
            department: str
            _labels = ['Person']
            _merge_keys = ['name']

            colleagues: Relationship = Relationship('Person', 'WORKS_WITH', 'Person')

        # Create test data
        alice = Person(name='Alice', department='Engineering')
        bob = Person(name='Bob', department='Product')
        charlie = Person(name='Charlie', department='Engineering')
        dave = Person(name='Dave', department='Sales')

        alice.colleagues.add(bob)
        alice.colleagues.add(charlie)
        alice.colleagues.add(dave)

        alice.merge()

        # Test string operations
        result = alice.colleagues.match(Person.department.starts_with('Eng')).all()
        assert len(result) == 1
        assert result[0].name == 'Charlie'

        # Test contains
        result = alice.colleagues.match(Person.name.contains('ob')).all()
        assert len(result) == 1
        assert result[0].name == 'Bob'

    def test_relationship_multiple_conditions(self, graph, test_base):
        """Test combining multiple filter conditions"""

        class Person(test_base.NodeModel):
            name: str
            age: int
            city: str
            _labels = ['Person']
            _merge_keys = ['name']

            friends: Relationship = Relationship('Person', 'FRIENDS', 'Person')

        # Create test data
        alice = Person(name='Alice', age=30, city='New York')
        bob = Person(name='Bob', age=40, city='London')
        charlie = Person(name='Charlie', age=35, city='London')
        dave = Person(name='Dave', age=45, city='Berlin')

        alice.friends.add(bob)
        alice.friends.add(charlie)
        alice.friends.add(dave)

        alice.merge()

        # Test multiple conditions
        result = alice.friends.match(
            Person.age > 30,
            Person.city == 'London'
        ).all()
        assert len(result) == 2
        assert set(p.name for p in result) == {'Bob', 'Charlie'}

        # Test combining different operators
        result = alice.friends.match(
            Person.age >= 40,
            Person.name.starts_with('D')
        ).all()
        assert len(result) == 1
        assert result[0].name == 'Dave'

    def test_relationship_no_matches(self, graph, test_base):
        """Test relationship query with no matches"""

        class Person(test_base.NodeModel):
            name: str
            age: int
            _labels = ['Person']
            _merge_keys = ['name']

            friends: Relationship = Relationship('Person', 'FRIENDS', 'Person')

        # Create test data
        alice = Person(name='Alice', age=30)
        bob = Person(name='Bob', age=40)

        alice.friends.add(bob)
        alice.merge()

        # Test with no matches
        result = alice.friends.match(Person.age > 80).all()
        assert len(result) == 0
        assert result == []

        # Test with impossible combination
        result = alice.friends.match(Person.name == 'Bob', Person.age < 30).all()
        assert len(result) == 0

    def test_relationship_match_without_filters(self, graph, test_base):
        """Test relationship match with no filters returns all related nodes"""

        class Person(test_base.NodeModel):
            name: str
            _labels = ['Person']
            _merge_keys = ['name']

            friends: Relationship = Relationship('Person', 'FRIENDS', 'Person')

        # Create test data
        alice = Person(name='Alice')
        bob = Person(name='Bob')
        charlie = Person(name='Charlie')

        alice.friends.add(bob)
        alice.friends.add(charlie)

        alice.merge()

        # Test without filters
        result = alice.friends.match().all()
        assert len(result) == 2
        assert set(p.name for p in result) == {'Bob', 'Charlie'}


class TestRelationshipPropertyFiltering:
    def test_relationship_filter_with_equality(self, graph, test_base):
        """Test filtering relationships by equality on relationship properties"""

        class Person(test_base.NodeModel):
            name: str
            age: int
            _labels = ['Person']
            _merge_keys = ['name']

            knows: Relationship = Relationship('Person', 'KNOWS', 'Person')

        # Create test data
        alice = Person(name='Alice', age=30)
        bob = Person(name='Bob', age=40)
        charlie = Person(name='Charlie', age=50)
        dave = Person(name='Dave', age=60)

        # Add relationships with properties
        alice.knows.add(bob, {'score': 85})
        alice.knows.add(charlie, {'score': 90})
        alice.knows.add(dave, {'score': 95})

        alice.merge()

        # Test filtering with equality
        result = alice.knows.filter(score=90).match().all()
        assert len(result) == 1
        assert result[0].name == 'Charlie'
        assert result[0].age == 50

        # Test filtering with equality and node property
        result = alice.knows.filter(score=95).match(Person.name == 'Dave').all()
        assert len(result) == 1
        assert result[0].name == 'Dave'
        assert result[0].age == 60

    def test_relationship_filter_with_relfield(self, graph, test_base):
        """Test filtering relationships using RelField comparison operators"""

        class Person(test_base.NodeModel):
            name: str
            age: int
            _labels = ['Person']
            _merge_keys = ['name']

            knows: Relationship = Relationship('Person', 'KNOWS', 'Person')

        # Create test data
        alice = Person(name='Alice', age=30)
        bob = Person(name='Bob', age=40)
        charlie = Person(name='Charlie', age=50)
        dave = Person(name='Dave', age=60)

        # Add relationships with properties
        alice.knows.add(bob, {'score': 85, 'since': 2020})
        alice.knows.add(charlie, {'score': 90, 'since': 2019})
        alice.knows.add(dave, {'score': 95, 'since': 2018})

        alice.merge()

        # Test filtering with greater than
        result = alice.knows.filter(RelField("score") > 85).match().all()
        assert len(result) == 2
        assert set(p.name for p in result) == {'Charlie', 'Dave'}

        # Test filtering with less than
        result = alice.knows.filter(RelField("score") < 95).match().all()
        assert len(result) == 2
        assert set(p.name for p in result) == {'Bob', 'Charlie'}

        # Test filtering with greater than or equal
        result = alice.knows.filter(RelField("score") >= 90).match().all()
        assert len(result) == 2
        assert set(p.name for p in result) == {'Charlie', 'Dave'}

        # Test filtering with less than or equal
        result = alice.knows.filter(RelField("score") <= 90).match().all()
        assert len(result) == 2
        assert set(p.name for p in result) == {'Bob', 'Charlie'}

    def test_relationship_filter_combined_conditions(self, graph, test_base):
        """Test combining multiple relationship property conditions"""

        class Person(test_base.NodeModel):
            name: str
            age: int
            _labels = ['Person']
            _merge_keys = ['name']

            knows: Relationship = Relationship('Person', 'KNOWS', 'Person')

        # Create test data
        alice = Person(name='Alice', age=30)
        bob = Person(name='Bob', age=40)
        charlie = Person(name='Charlie', age=50)
        dave = Person(name='Dave', age=60)

        # Add relationships with multiple properties
        alice.knows.add(bob, {'score': 85, 'since': 2020, 'type': 'friend'})
        alice.knows.add(charlie, {'score': 90, 'since': 2019, 'type': 'colleague'})
        alice.knows.add(dave, {'score': 95, 'since': 2018, 'type': 'friend'})

        alice.merge()

        # Test filtering with multiple relationship properties
        result = alice.knows.filter(
            RelField("score") > 85,
            type="friend"
        ).match().all()
        assert len(result) == 1
        assert result[0].name == 'Dave'

        # Test relationship filter with node property filter
        result = alice.knows.filter(
            RelField("since") < 2020
        ).match(
            Person.age > 45
        ).all()
        assert len(result) == 2
        assert set(p.name for p in result) == {'Charlie', 'Dave'}

    def test_relationship_filter_string_operations(self, graph, test_base):
        """Test string operations on relationship properties"""

        class Person(test_base.NodeModel):
            name: str
            _labels = ['Person']
            _merge_keys = ['name']

            knows: Relationship = Relationship('Person', 'KNOWS', 'Person')

        # Create test data
        alice = Person(name='Alice')
        bob = Person(name='Bob')
        charlie = Person(name='Charlie')
        dave = Person(name='Dave')

        # Add relationships with string properties
        alice.knows.add(bob, {'category': 'family'})
        alice.knows.add(charlie, {'category': 'friend'})
        alice.knows.add(dave, {'category': 'coworker'})

        alice.merge()

        # Test starts_with
        result = alice.knows.filter(
            RelField("category").starts_with("f")
        ).match().all()
        assert len(result) == 2
        assert set(p.name for p in result) == {'Bob', 'Charlie'}

        # Test contains
        result = alice.knows.filter(
            RelField("category").contains("work")
        ).match().all()
        assert len(result) == 1
        assert result[0].name == 'Dave'

        # Test ends_with
        result = alice.knows.filter(
            RelField("category").ends_with("ly")
        ).match().all()
        assert len(result) == 1
        assert result[0].name == 'Bob'

    def test_relationship_filter_with_no_matches(self, graph, test_base):
        """Test relationship filtering with no matches"""

        class Person(test_base.NodeModel):
            name: str
            age: int
            _labels = ['Person']
            _merge_keys = ['name']

            knows: Relationship = Relationship('Person', 'KNOWS', 'Person')

        # Create test data
        alice = Person(name='Alice', age=30)
        bob = Person(name='Bob', age=40)

        alice.knows.add(bob, {'score': 85})
        alice.merge()

        # Test filtering with impossible condition
        result = alice.knows.filter(RelField("score") > 100).match().all()
        assert len(result) == 0

        # Test filtering with non-existent property
        result = alice.knows.filter(non_existent="value").match().all()
        assert len(result) == 0

    def test_relationship_filter_chained_with_node_filter(self, graph, test_base):
        """Test chaining relationship filters with node filters"""

        class Person(test_base.NodeModel):
            name: str
            age: int
            city: str
            _labels = ['Person']
            _merge_keys = ['name']

            knows: Relationship = Relationship('Person', 'KNOWS', 'Person')

        # Create test data
        alice = Person(name='Alice', age=30, city='New York')
        bob = Person(name='Bob', age=40, city='London')
        charlie = Person(name='Charlie', age=50, city='London')
        dave = Person(name='Dave', age=60, city='Berlin')

        # Add relationships with properties
        alice.knows.add(bob, {'score': 85, 'since': 2020})
        alice.knows.add(charlie, {'score': 90, 'since': 2019})
        alice.knows.add(dave, {'score': 95, 'since': 2018})

        alice.merge()

        # Test combining relationship filters with node filters
        result = alice.knows.filter(
            RelField("score") >= 90
        ).match(
            Person.city == 'London'
        ).all()
        assert len(result) == 1
        assert result[0].name == 'Charlie'

        # Test with multiple node conditions
        result = alice.knows.filter(
            RelField("since") <= 2019
        ).match(
            Person.age > 45,
            Person.city != 'New York'
        ).all()
        assert len(result) == 2
        assert set(p.name for p in result) == {'Charlie', 'Dave'}

    def test_relationship_filter_inequality(self, graph, test_base):
        """Test relationship filtering with inequality operators"""

        class Person(test_base.NodeModel):
            name: str
            _labels = ['Person']
            _merge_keys = ['name']

            knows: Relationship = Relationship('Person', 'KNOWS', 'Person')

        # Create test data
        alice = Person(name='Alice')
        bob = Person(name='Bob')
        charlie = Person(name='Charlie')

        # Add relationships with properties
        alice.knows.add(bob, {'status': 'active'})
        alice.knows.add(charlie, {'status': 'inactive'})

        alice.merge()

        # Test with inequality
        result = alice.knows.filter(
            RelField("status") != 'active'
        ).match().all()
        assert len(result) == 1
        assert result[0].name == 'Charlie'


class TestNodeModelMatchFirst:
    def test_first_returns_only_one_node(self, test_base, graph):
        """Test that first() returns only one node when multiple would match"""

        class Person(test_base.NodeModel):
            name: str
            age: int

            _labels = ['Person']
            _merge_keys = ['name']

        # Create test data - multiple matching nodes
        Person(name='John', age=30).merge()
        Person(name='Sarah', age=30).merge()
        Person(name='Peter', age=30).merge()

        # Should return just one person with age 30
        result = Person.match(age=30).first()

        # Assert we got a single Person instance
        assert isinstance(result, Person)
        assert result.age == 30

        # All people have the same age, so any one could be returned
        assert result.name in ['John', 'Sarah', 'Peter']

    def test_first_returns_none_when_no_matches(self, test_base, graph):
        """Test that first() returns None when no nodes match"""

        class Person(test_base.NodeModel):
            name: str
            age: int

            _labels = ['Person']
            _merge_keys = ['name']

        # Create test data
        Person(name='John', age=30).merge()

        # Should return None for a non-matching query
        result = Person.match(name='NonExistent').first()

        # Assert we got None
        assert result is None

    def test_first_with_filter_conditions(self, test_base, graph):
        """Test that first() works with filter conditions"""

        class Person(test_base.NodeModel):
            name: str
            age: int

            _labels = ['Person']
            _merge_keys = ['name']

        # Create test data
        Person(name='John', age=30).merge()
        Person(name='Sarah', age=25).merge()
        Person(name='Peter', age=40).merge()

        # Filter with specific condition
        result = Person.match(FilterOp("age", ">", 30)).first()

        # Should return Peter
        assert isinstance(result, Person)
        assert result.name == 'Peter'
        assert result.age == 40

    def test_first_with_class_attributes(self, test_base, graph):
        """Test that first() works with class attribute filtering"""

        class Person(test_base.NodeModel):
            name: str
            age: int

            _labels = ['Person']
            _merge_keys = ['name']

        # Create test data
        Person(name='John', age=30).merge()
        Person(name='Sarah', age=25).merge()
        Person(name='Peter', age=40).merge()

        # Use class attribute for filtering
        result = Person.match(Person.name == 'Sarah').first()

        # Should return Sarah
        assert isinstance(result, Person)
        assert result.name == 'Sarah'
        assert result.age == 25

    def test_first_with_cypher_query(self, test_base, graph):
        """Test that first() works with custom Cypher queries"""

        class Person(test_base.NodeModel):
            name: str
            age: int

            _labels = ['Person']
            _merge_keys = ['name']

        # Create test data
        Person(name='John', age=30).merge()
        Person(name='Sarah', age=25).merge()
        Person(name='Peter', age=40).merge()

        # Use custom Cypher query
        query = """
        MATCH (n:Person)
        WHERE n.age > $min_age
        ORDER BY n.age ASC
        RETURN n
        """
        result = Person.match(CypherQuery(query, params={"min_age": 25})).first()

        # Should return John (lowest age above 25)
        assert isinstance(result, Person)
        assert result.name == 'John'
        assert result.age == 30