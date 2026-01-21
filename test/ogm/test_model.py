import pytest
import random
from datetime import datetime, timedelta

from graphio.utils import run_query_return_results
from graphio import Relationship, NodeSet, RelationshipSet, FilterOp, CypherQuery, RelField, NodeModel


class TestRegistryMeta:
    def test_registry_meta(self, test_base):
        # Define a class using the Base from the test_base fixture
        class MyNode(NodeModel):
            name: str

            _labels = ['Person']
            _merge_keys = ['name']

        # With simplified registry, classes are automatically registered when defined
        # Test class registration and lookup
        retrieved_class = test_base.get_class_by_name('MyNode')

        # Assertions
        assert retrieved_class is not None
        assert retrieved_class == MyNode


class TestCreateIndex:
    def test_create_index(self, graph, test_base):
        class TestNode(NodeModel):
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

        class Person(NodeModel):
            name: str
            age: int

            _labels = ['Person']
            _merge_keys = ['name']

        persons = Person.nodeset()

        assert isinstance(persons, NodeSet)

        for i in range(10):
            persons.add({'name': f'Person {i}', 'age': i})

        assert len(persons.nodes) == 10

    def test_relationshipset_from_nodemodel(self, test_base):
        """
        Test if we can add data to a relationshipset from a nodemodel
        """

        class Person(NodeModel):
            name: str
            age: int

            _labels = ['Person']
            _merge_keys = ['name']

            friends: Relationship = Relationship('Person', 'FRIENDS', 'Person')

        friends = Person.friends.dataset()
        assert isinstance(friends, RelationshipSet)

        for i in range(10):
            friends.add({'name': f'Person {i}', 'age': i}, {'name': f'Person {i - 1}', 'age': i - 1},
                                     {'since': i})

        assert len(friends.relationships) == 10


class TestNodeModelCreateMerge:

    def test_unique_id_dict_basic(self, test_base):
        """Test that match_dict correctly returns dictionary of merge keys and values"""

        class User(NodeModel):
            _labels = ["User"]
            _merge_keys = ["username"]
            username: str
            age: int = None

        user = User(username="alice", age=30)

        # Test that match_dict returns the correct dictionary
        assert user.match_dict == {"username": "alice"}
        assert "age" not in user.match_dict

    def test_unique_id_dict_with_inheritance(self, test_base):
        """Test that match_dict works with class inheritance"""

        class Person(NodeModel):
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
        assert employee.match_dict == {"id": "p123", "employee_id": "e456"}
        assert "name" not in employee.match_dict
        assert "department" not in employee.match_dict

    def test_merge_keys_validation(self, test_base):
        with pytest.raises(ValueError, match="Merge key 'invalid_key' is not a valid model field."):
            class InvalidNodeModel(NodeModel):
                name: str
                age: int
                _merge_keys = ['invalid_key']

            InvalidNodeModel(name="example", age=30)

    def test_match_dict_on_class(self, test_base):
        class MyNode(NodeModel):
            name: str
            something: str
            _labels = ['Person']
            _merge_keys = ['name']

        node_model = MyNode(name='John', something='other')
        assert node_model.match_dict == {'name': 'John'}

    def test_model_create(self, test_base, graph):
        class TestNode(NodeModel):
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
        class TestNode(NodeModel):
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
        class TestNode(NodeModel):
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
        class Person(NodeModel):
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
        class Person(NodeModel):
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

        class Person(NodeModel):
            name: str
            _labels = ['Person']
            _merge_keys = ['name']

            lives_in: Relationship = Relationship('Person', 'FRIENDS', 'City')

        class City(NodeModel):
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
        class Person(NodeModel):
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
        class Person(NodeModel):
            name: str

            _labels = ['Person']
            _merge_keys = ['name']

            lives_in: Relationship = Relationship('Person', 'LIVES_IN', 'City')

        class City(NodeModel):
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
        class Person(NodeModel):
            name: str
            _labels = ['Person']
            _merge_keys = ['name']

            lives_in: Relationship = Relationship('Person', 'LIVES_IN', 'City')

        class City(NodeModel):
            name: str
            _labels = ['City']
            _merge_keys = ['name']

            located_in: Relationship = Relationship('City', 'LOCATED_IN', 'Country')

        class Country(NodeModel):
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
        class Person(NodeModel):
            name: str

            _labels = ['Person']
            _merge_keys = ['name']

            lives_in: Relationship = Relationship('Person', 'LIVES_IN', 'City')

        class City(NodeModel):
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
        class Person(NodeModel):
            name: str

            _labels = ['Person']
            _merge_keys = ['name']

            lives_in: Relationship = Relationship('Person', 'LIVES_IN', 'City')

        class City(NodeModel):
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
        class Person(NodeModel):
            name: str

            _labels = ['Person']
            _merge_keys = ['name']

            lives_in: Relationship = Relationship('Person', 'LIVES_IN', 'City')
            friends: Relationship = Relationship('Person', 'FRIENDS', 'Person')

        class City(NodeModel):
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
        class Person(NodeModel):
            name: str

            _labels = ['Person']
            _merge_keys = ['name']

            lives_in: Relationship = Relationship('Person', 'LIVES_IN', 'City')

        class City(NodeModel):
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

    def test_reverse_relationship_creation_same_result(self, graph, test_base):
        """Test that creating relationships from both sides produces the same result"""
        
        class Supplier(NodeModel):
            _labels = ['Supplier']
            _merge_keys = ['name']
            name: str
            
            supplier_lists: Relationship = Relationship('Supplier', 'HAS_SUPPLIER_LIST', 'SupplierList')

        class SupplierList(NodeModel):
            _labels = ['SupplierList']
            _merge_keys = ['id']
            id: str
            filename: str
            
            from_supplier: Relationship = Relationship('Supplier', 'HAS_SUPPLIER_LIST', 'SupplierList')
        
        # Test 1: Create from Supplier side
        supplier1 = Supplier(name="ACME Corp")
        supplier_list1 = SupplierList(id="list-123", filename="products.csv")
        supplier1.supplier_lists.add(supplier_list1)
        supplier1.merge()
        
        # Test 2: Create from SupplierList side
        supplier2 = Supplier(name="TechCorp")
        supplier_list2 = SupplierList(id="list-456", filename="services.csv")
        supplier_list2.from_supplier.add(supplier2)
        supplier_list2.merge()
        
        # Both should create relationships in the same direction
        result = run_query_return_results(graph, """
            MATCH (s:Supplier)-[r:HAS_SUPPLIER_LIST]->(sl:SupplierList) 
            RETURN s.name, sl.id 
            ORDER BY s.name
        """)
        
        assert len(result) == 2
        assert result[0][0] == "ACME Corp"
        assert result[0][1] == "list-123"
        assert result[1][0] == "TechCorp"
        assert result[1][1] == "list-456"
        
        # Verify no relationships in wrong direction
        result = run_query_return_results(graph, "MATCH (sl:SupplierList)-[r:HAS_SUPPLIER_LIST]->(s:Supplier) RETURN count(r)")
        assert result[0][0] == 0

    def test_reverse_relationship_creation_from_both_sides(self, graph, test_base):
        """Test that creating relationships from both sides works correctly"""
        
        class Company(NodeModel):
            _labels = ['Company']
            _merge_keys = ['name']
            name: str
            
            departments: Relationship = Relationship('Company', 'HAS_DEPARTMENT', 'Department')

        class Department(NodeModel):
            _labels = ['Department']
            _merge_keys = ['name']
            name: str
            
            company: Relationship = Relationship('Company', 'HAS_DEPARTMENT', 'Department')
        
        # Create relationship from department side
        company = Company(name="TechCorp")
        department = Department(name="Engineering")
        
        department.company.add(company)
        department.merge()
        
        # Verify relationship was created in correct direction
        result = run_query_return_results(graph, """
            MATCH (c:Company)-[r:HAS_DEPARTMENT]->(d:Department) 
            RETURN c.name, d.name
        """)
        assert len(result) == 1
        assert result[0][0] == "TechCorp"
        assert result[0][1] == "Engineering"
        
        # Verify no reverse direction relationships exist
        result = run_query_return_results(graph, "MATCH (d:Department)-[r:HAS_DEPARTMENT]->(c:Company) RETURN count(r)")
        assert result[0][0] == 0


class TestNodeModelMatch:

    def test_node_match_without_properties(self, test_base, graph):
        class Person(NodeModel):
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
        class Person(NodeModel):
            name: str
            _labels = ['Person']
            _merge_keys = ['name']

        john = Person(name='John')
        john.merge()

        result = run_query_return_results(graph, 'MATCH (m:Person) RETURN m')
        assert result[0][0]['name'] == 'John'

        result = Person.match(Person.name == 'John').all()

        assert all([x.name == 'John' for x in result])
        assert all(isinstance(x, Person) for x in result)

    def test_node_match_multiple_properties(self, test_base, graph):
        class Person(NodeModel):
            name: str
            age: int

            _labels = ['Person']
            _merge_keys = ['name']

        john = Person(name='John', age=30)
        john.merge()

        peter = Person(name='Peter', age=40)
        peter.merge()

        result = Person.match(Person.name == 'John', Person.age == 30).all()

        assert all([x.name == 'John' for x in result])
        assert all([x.age == 30 for x in result])
        assert all(isinstance(x, Person) for x in result)

    def test_node_match_no_data(self, test_base, graph):
        class Person(NodeModel):
            name: str
            age: int

            _labels = ['Person']
            _merge_keys = ['name']

        result = Person.match(Person.name == 'John').all()

        assert result == []

    def test_node_match_with_addtional_properties(self, test_base):
        class Person(NodeModel):
            name: str

            _labels = ['Person']
            _merge_keys = ['name']

        john = Person(name='John', age=30)
        john.merge()

        result = Person.match(Person.name == 'John', FilterOp('age', '=', 30)).all()

        assert all([x.name == 'John' for x in result])
        assert all([x.age == 30 for x in result])
        assert all(isinstance(x, Person) for x in result)


    def test_matching_relationships(self, graph, test_base):
        class Person(NodeModel):
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

        class Person(NodeModel):
            name: str
            age: int

            _labels = ['Person']
            _merge_keys = ['name']

        # Create test data
        Person(name='John', age=30).merge()
        Person(name='Peter', age=40).merge()
        Person(name='Sarah', age=30).merge()

        # Test filtering with equality
        result = Person.match(Person.name == 'John').all()
        assert len(result) == 1
        assert result[0].name == 'John'
        assert result[0].age == 30

        # Test filtering with multiple equality conditions
        result = Person.match(Person.age == 30).all()
        assert len(result) == 2
        assert {p.name for p in result} == {'John', 'Sarah'}

    def test_complex_filtering_with_filter_op(self, graph, test_base):
        """Test filtering using FilterOp for complex conditions"""

        class Person(NodeModel):
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

        class Person(NodeModel):
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

        class Person(NodeModel):
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

        class Person(NodeModel):
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

        class Person(NodeModel):
            name: str
            age: int

            _labels = ['Person']
            _merge_keys = ['name']

            friends: Relationship = Relationship('Person', 'FRIENDS', 'Person')
            lives_in: Relationship = Relationship('Person', 'LIVES_IN', 'City')

        class City(NodeModel):
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

        class Person(NodeModel):
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

        class Person(NodeModel):
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

        class Person(NodeModel):
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

        class Person(NodeModel):
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

        # The additional filter should be ignored when CypherQuery is present
        result = Person.match(CypherQuery(query)).all()
        assert len(result) == 1
        assert result[0].name == 'Peter'  # Not 'John'
        assert result[0].age == 40

    def test_invalid_cypher_query_variable(self, graph, test_base):
        """Test that using an invalid variable name raises an error"""

        class Person(NodeModel):
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

        class Person(NodeModel):
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

        class Person(NodeModel):
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

        class Product(NodeModel):
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

        class Person(NodeModel):
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

        class Person(NodeModel):
            name: str

            _labels = ['Person']
            _merge_keys = ['name']

        # This should raise an error because 'age' is not a field
        with pytest.raises(AttributeError):
            Person.match(Person.age == 30).all()

    def test_date_equality_filtering(self, graph, test_base):
        """Test date equality filtering"""
        from datetime import date

        class Event(NodeModel):
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

        class Event(NodeModel):
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

        class LogEntry(NodeModel):
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

        class LogEntry(NodeModel):
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

        class Appointment(NodeModel):
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

        class Person(NodeModel):
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
        result = alice.friends.match(Person.age == 50).all()
        assert len(result) == 1
        assert result[0].name == 'Charlie'

        # Test with multiple matches
        result = alice.friends.match(Person.age == 60).all()  # Should get Charlie and Dave
        assert len(result) == 1
        assert result[0].name == 'Dave'

    def test_relationship_field_descriptor_filtering(self, graph, test_base):
        """Test filtering relationships with field descriptors"""

        class Person(NodeModel):
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

        class Person(NodeModel):
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

        class Person(NodeModel):
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

        class Person(NodeModel):
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

        class Person(NodeModel):
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

    def test_relationship_first_match(self, graph, test_base):
        class Person(NodeModel):
            name: str
            age: int
            _labels = ['Person']
            _merge_keys = ['name']

            friends: Relationship = Relationship('Person', 'FRIENDS', 'Person')

        # Create test data
        alice = Person(name='Alice', age=30)
        bob = Person(name='Bob', age=40)
        charlie = Person(name='Charlie', age=50)

        alice.friends.add(bob)
        alice.friends.add(charlie)

        alice.merge()

        # Test first match
        first_friend = alice.friends.match().first()
        assert first_friend is not None
        assert first_friend.name in {'Bob', 'Charlie'}

    def test_relationship_first_no_match(self, graph, test_base):
        class Person(NodeModel):
            name: str
            age: int
            _labels = ['Person']
            _merge_keys = ['name']

            friends: Relationship = Relationship('Person', 'FRIENDS', 'Person')

        # Create test data
        alice = Person(name='Alice', age=30)
        bob = Person(name='Bob', age=40)

        alice.merge()
        bob.merge()

        # Test first match with no relationships
        first_friend = alice.friends.match().first()
        assert first_friend is None

    def test_reverse_relationship_querying_basic(self, graph, test_base):
        """Test basic reverse relationship querying functionality"""
        
        class Author(NodeModel):
            _labels = ['Author']
            _merge_keys = ['name']
            name: str
            
            books: Relationship = Relationship('Author', 'WROTE', 'Book')

        class Book(NodeModel):
            _labels = ['Book']
            _merge_keys = ['title']
            title: str
            
            # Same relationship definition - should work in reverse
            author: Relationship = Relationship('Author', 'WROTE', 'Book')
        
        # Create test data from author side only
        author = Author(name="Isaac Asimov")
        book1 = Book(title="Foundation")
        book2 = Book(title="I, Robot")
        
        author.books.add(book1)
        author.books.add(book2)
        author.merge()
        
        # Test forward querying (should work as before)
        loaded_author = Author.match(Author.name == "Isaac Asimov").first()
        books = loaded_author.books.match().all()
        assert len(books) == 2
        book_titles = {book.title for book in books}
        assert book_titles == {"Foundation", "I, Robot"}
        
        # Test reverse querying (new functionality)
        loaded_book = Book.match(Book.title == "Foundation").first()
        authors = loaded_book.author.match().all()
        assert len(authors) == 1
        assert authors[0].name == "Isaac Asimov"

    def test_reverse_relationship_querying_with_filters(self, graph, test_base):
        """Test reverse relationship querying with filters"""
        
        class Customer(NodeModel):
            _labels = ['Customer']
            _merge_keys = ['email']
            email: str
            name: str
            age: int
            
            orders: Relationship = Relationship('Customer', 'PLACED', 'Order')

        class Order(NodeModel):
            _labels = ['Order']
            _merge_keys = ['id']
            id: str
            amount: float
            
            customer: Relationship = Relationship('Customer', 'PLACED', 'Order')
        
        # Create test data
        customer1 = Customer(email="alice@example.com", name="Alice", age=30)
        customer2 = Customer(email="bob@example.com", name="Bob", age=25)
        
        order1 = Order(id="order-1", amount=100.0)
        order2 = Order(id="order-2", amount=200.0)
        order3 = Order(id="order-3", amount=50.0)
        
        customer1.orders.add(order1)
        customer1.orders.add(order2)
        customer2.orders.add(order3)
        
        customer1.merge()
        customer2.merge()
        
        # Test reverse query with filters on target node
        loaded_order = Order.match(Order.id == "order-1").first()
        customers = loaded_order.customer.match(Customer.age > 28).all()
        assert len(customers) == 1
        assert customers[0].name == "Alice"
        
        # Test reverse query that should return empty
        loaded_order = Order.match(Order.id == "order-3").first()
        customers = loaded_order.customer.match(Customer.age > 28).all()
        assert len(customers) == 0

    def test_reverse_relationship_querying_with_relationship_properties(self, graph, test_base):
        """Test reverse relationship querying with relationship property filters"""
        
        class Student(NodeModel):
            _labels = ['Student']
            _merge_keys = ['id']
            id: str
            name: str
            
            enrollments: Relationship = Relationship('Student', 'ENROLLED_IN', 'Course')

        class Course(NodeModel):
            _labels = ['Course']
            _merge_keys = ['code']
            code: str
            name: str
            
            students: Relationship = Relationship('Student', 'ENROLLED_IN', 'Course')
        
        # Create test data with relationship properties
        student = Student(id="s123", name="Alice")
        course1 = Course(code="CS101", name="Intro to CS")
        course2 = Course(code="CS201", name="Data Structures")
        
        # Add relationships with properties
        student.enrollments.add(course1, {"grade": "A", "semester": "Fall2023"})
        student.enrollments.add(course2, {"grade": "B+", "semester": "Spring2024"})
        student.merge()
        
        # Test reverse query with relationship filters
        loaded_course = Course.match(Course.code == "CS101").first()
        students = loaded_course.students.filter(RelField("grade") == "A").match().all()
        assert len(students) == 1
        assert students[0].name == "Alice"
        
        # Test reverse query with relationship filters that don't match
        loaded_course = Course.match(Course.code == "CS201").first()
        students = loaded_course.students.filter(RelField("grade") == "A").match().all()
        assert len(students) == 0

    def test_reverse_relationship_does_not_affect_self_referencing(self, graph, test_base):
        """Test that reverse relationship detection doesn't break self-referencing relationships"""
        
        class Person(NodeModel):
            _labels = ['Person']
            _merge_keys = ['name']
            name: str
            
            # Self-referencing relationship should always work normally
            friends: Relationship = Relationship('Person', 'FRIENDS', 'Person')
            mentors: Relationship = Relationship('Person', 'MENTORS', 'Person')
        
        # Create test data
        alice = Person(name="Alice")
        bob = Person(name="Bob")
        charlie = Person(name="Charlie")
        
        # Alice is friends with Bob, Bob mentors Charlie
        alice.friends.add(bob)
        bob.mentors.add(charlie)
        
        alice.merge()
        bob.merge()
        
        # Test that self-referencing relationships work normally
        loaded_alice = Person.match(Person.name == "Alice").first()
        friends = loaded_alice.friends.match().all()
        assert len(friends) == 1
        assert friends[0].name == "Bob"
        
        loaded_bob = Person.match(Person.name == "Bob").first()
        mentees = loaded_bob.mentors.match().all()
        assert len(mentees) == 1
        assert mentees[0].name == "Charlie"


class TestRelationshipPropertyFiltering:
    def test_relationship_filter_with_equality(self, graph, test_base):
        """Test filtering relationships by equality on relationship properties"""

        class Person(NodeModel):
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
        result = alice.knows.filter(RelField("score") == 90).match().all()
        assert len(result) == 1
        assert result[0].name == 'Charlie'
        assert result[0].age == 50

        # Test filtering with equality and node property
        result = alice.knows.filter(RelField("score") == 95).match(Person.name == 'Dave').all()
        assert len(result) == 1
        assert result[0].name == 'Dave'
        assert result[0].age == 60

    def test_relationship_filter_with_relfield(self, graph, test_base):
        """Test filtering relationships using RelField comparison operators"""

        class Person(NodeModel):
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

        class Person(NodeModel):
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
            RelField("type") == "friend"
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

        class Person(NodeModel):
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

        class Person(NodeModel):
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
        result = alice.knows.filter(RelField("non_existent") == "value").match().all()
        assert len(result) == 0

    def test_relationship_filter_chained_with_node_filter(self, graph, test_base):
        """Test chaining relationship filters with node filters"""

        class Person(NodeModel):
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

        class Person(NodeModel):
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

        class Person(NodeModel):
            name: str
            age: int

            _labels = ['Person']
            _merge_keys = ['name']

        # Create test data - multiple matching nodes
        Person(name='John', age=30).merge()
        Person(name='Sarah', age=30).merge()
        Person(name='Peter', age=30).merge()

        # Should return just one person with age 30
        result = Person.match(Person.age == 30).first()

        # Assert we got a single Person instance
        assert isinstance(result, Person)
        assert result.age == 30

        # All people have the same age, so any one could be returned
        assert result.name in ['John', 'Sarah', 'Peter']

    def test_first_returns_none_when_no_matches(self, test_base, graph):
        """Test that first() returns None when no nodes match"""

        class Person(NodeModel):
            name: str
            age: int

            _labels = ['Person']
            _merge_keys = ['name']

        # Create test data
        Person(name='John', age=30).merge()

        # Should return None for a non-matching query
        result = Person.match(Person.name == 'NonExistent').first()

        # Assert we got None
        assert result is None

    def test_first_with_filter_conditions(self, test_base, graph):
        """Test that first() works with filter conditions"""

        class Person(NodeModel):
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

        class Person(NodeModel):
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

        class Person(NodeModel):
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


class TestClassLevelRelationshipMatch:
    def test_basic_class_level_relationship_match(self, test_base, graph):
        """Test basic class-level relationship matching"""

        class Person(NodeModel):
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

        # Add relationships
        alice.knows.add(bob)
        alice.knows.add(charlie)
        bob.knows.add(charlie)

        # Merge all nodes and relationships
        alice.merge()
        bob.merge()
        charlie.merge()

        # test with raw Cypher query if we have 3 relationships
        query = """
        MATCH (n:Person)-[r:KNOWS]->(m:Person)
        RETURN count(r) as count
        """
        result = run_query_return_results(graph, query)
        print(result)
        assert len(result) == 1
        assert result[0]['count'] == 3

        # Test class-level relationship matching
        all_friends = Person.match().knows.match().all()

        # Should find all target notes (bob and charlie)
        assert len(all_friends) == 2
        names = sorted([p.name for p in all_friends])
        assert names == ['Bob', 'Charlie']

    def test_relationship_filtering(self, test_base, graph):
        """Test filtering on relationship properties"""

        class Person(NodeModel):
            name: str
            age: int
            _labels = ['Person']
            _merge_keys = ['name']

            knows: Relationship = Relationship('Person', 'KNOWS', 'Person')

        # Create test data
        alice = Person(name='Alice', age=30)
        bob = Person(name='Bob', age=40)
        charlie = Person(name='Charlie', age=50)

        # Add relationships with properties
        alice.knows.add(bob, {'score': 85})
        alice.knows.add(charlie, {'score': 95})

        # Merge all nodes and relationships
        alice.merge()
        bob.merge()
        charlie.merge()

        # Test filtering on relationship properties
        high_score_friends = Person.match().knows.filter(RelField("score") > 90).match().all()

        assert len(high_score_friends) == 1
        assert high_score_friends[0].name == 'Charlie'

    def test_source_node_filtering(self, test_base, graph):
        """Test filtering on source node properties"""

        class Person(NodeModel):
            name: str
            age: int
            _labels = ['Person']
            _merge_keys = ['name']

            knows: Relationship = Relationship('Person', 'KNOWS', 'Person')

        # Create test data
        alice = Person(name='Alice', age=30)
        bob = Person(name='Bob', age=40)
        charlie = Person(name='Charlie', age=50)
        dave = Person(name='Dave', age=35)

        # Add relationships
        alice.knows.add(bob)
        alice.knows.add(charlie)
        bob.knows.add(dave)
        charlie.knows.add(dave)

        # Merge all nodes and relationships
        alice.merge()
        bob.merge()
        charlie.merge()
        dave.merge()

        # Test filtering on source node
        young_peoples_friends = Person.match(Person.age < 40).knows.match().all()

        # Should find friends of young people (Alice)
        assert len(young_peoples_friends) == 2
        names = sorted([p.name for p in young_peoples_friends])
        assert names == ['Bob', 'Charlie']  # Alice knows Bob and Charlie

    def test_combined_filtering(self, test_base, graph):
        """Test filtering on source, relationship, and target properties"""

        class Person(NodeModel):
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
        dave = Person(name='Dave', age=35, city='Berlin')
        eve = Person(name='Eve', age=45, city='Paris')

        # Add relationships with properties
        alice.knows.add(bob, {'score': 85})
        alice.knows.add(charlie, {'score': 95})
        bob.knows.add(dave, {'score': 90})
        bob.knows.add(eve, {'score': 75})

        # Merge all nodes and relationships
        alice.merge()
        bob.merge()
        charlie.merge()
        dave.merge()
        eve.merge()

        # Test complex filtering combining source, relationship and target filters
        result = Person.match(
            Person.city == 'New York'
        ).knows.filter(
            RelField("score") >= 90
        ).match(
            Person.age > 45
        ).all()

        assert len(result) == 1
        assert result[0].name == 'Charlie'

    def test_first_method(self, test_base, graph):
        """Test first() method with class-level relationship queries"""

        class Person(NodeModel):
            name: str
            age: int
            _labels = ['Person']
            _merge_keys = ['name']

            knows: Relationship = Relationship('Person', 'KNOWS', 'Person')

        # Create test data
        alice = Person(name='Alice', age=30)
        bob = Person(name='Bob', age=40)
        charlie = Person(name='Charlie', age=50)

        # Add relationships
        alice.knows.add(bob)
        alice.knows.add(charlie)

        # Merge all nodes and relationships
        alice.merge()
        bob.merge()
        charlie.merge()

        # Test first() with filters
        first_older_friend = Person.match().knows.match(Person.age > 45).first()
        assert first_older_friend is not None
        assert first_older_friend.name == 'Charlie'

    def test_no_results(self, test_base, graph):
        """Test behavior when no results match the query"""

        class Person(NodeModel):
            name: str
            age: int
            _labels = ['Person']
            _merge_keys = ['name']

            knows: Relationship = Relationship('Person', 'KNOWS', 'Person')

        # Create test data but no relationships
        alice = Person(name='Alice', age=30)
        bob = Person(name='Bob', age=40)

        # Merge nodes only
        alice.merge()
        bob.merge()

        # Test with no relationships
        friends = Person.match().knows.match().all()
        assert len(friends) == 0

        # Test first() with no relationships
        first_friend = Person.match().knows.match().first()
        assert first_friend is None


class TestRelationshipDataset:
    """Test Relationship.dataset() method"""
    
    def test_relationship_dataset_configuration(self, test_base):
        """Test that Relationship.dataset() creates correctly configured RelationshipSet"""
        class Person(NodeModel):
            _labels = ['Person']
            _merge_keys = ['email']
            name: str
            email: str
            age: int
            
            knows: Relationship = Relationship('Person', 'KNOWS', 'Person')
            works_at: Relationship = Relationship('Person', 'WORKS_AT', 'Company')
        
        class Company(NodeModel):
            _labels = ['Company'] 
            _merge_keys = ['name']
            name: str
            industry: str
        
        # Test Person -> Person relationship
        friendship_dataset = Person.knows.dataset()
        assert friendship_dataset.rel_type == 'KNOWS'
        assert friendship_dataset.start_node_labels == ['Person']
        assert friendship_dataset.end_node_labels == ['Person']
        assert friendship_dataset.start_node_properties == ['email']
        assert friendship_dataset.end_node_properties == ['email']
        
        # Test Person -> Company relationship
        employment_dataset = Person.works_at.dataset()
        assert employment_dataset.rel_type == 'WORKS_AT'
        assert employment_dataset.start_node_labels == ['Person']
        assert employment_dataset.end_node_labels == ['Company']
        assert employment_dataset.start_node_properties == ['email']
        assert employment_dataset.end_node_properties == ['name']
    
    def test_relationship_dataset_usage(self, test_base):
        """Test using RelationshipSet created from Relationship.dataset()"""
        class Person(NodeModel):
            _labels = ['Person']
            _merge_keys = ['email']
            name: str
            email: str
            age: int
            
            knows: Relationship = Relationship('Person', 'KNOWS', 'Person')
        
        # Create dataset and add relationships
        friendship_dataset = Person.knows.dataset()
        
        alice = Person(name='Alice', email='alice@example.com', age=30)
        bob = Person(name='Bob', email='bob@example.com', age=25)
        
        friendship_dataset.add(alice, bob, {'since': '2020', 'strength': 0.8})
        
        # Verify relationship was added
        assert len(friendship_dataset.relationships) == 1
        start_props, end_props, rel_props = friendship_dataset.relationships[0]
        
        assert start_props == {'email': 'alice@example.com'}
        assert end_props == {'email': 'bob@example.com'}
        assert rel_props == {'since': '2020', 'strength': 0.8}
    
    def test_relationship_dataset_multiple_relationships(self, test_base):
        """Test Relationship.dataset() with multiple different relationship types"""
        class Person(NodeModel):
            _labels = ['Person']
            _merge_keys = ['email']
            name: str
            email: str
            
            knows: Relationship = Relationship('Person', 'KNOWS', 'Person')
            manages: Relationship = Relationship('Person', 'MANAGES', 'Person')
        
        class Company(NodeModel):
            _labels = ['Company']
            _merge_keys = ['name']
            name: str
            
            employs: Relationship = Relationship('Company', 'EMPLOYS', 'Person')
        
        # Each relationship should create a different dataset
        friendship = Person.knows.dataset()
        management = Person.manages.dataset()
        employment = Company.employs.dataset()
        
        # Verify they're different instances
        assert friendship is not management
        assert management is not employment
        
        # Verify configurations
        assert friendship.rel_type == 'KNOWS'
        assert management.rel_type == 'MANAGES'
        assert employment.rel_type == 'EMPLOYS'
        
        # Verify directional configurations
        assert employment.start_node_labels == ['Company']
        assert employment.end_node_labels == ['Person']
        assert employment.start_node_properties == ['name']
        assert employment.end_node_properties == ['email']
    
    def test_relationship_dataset_integration_with_bulk_operations(self, graph, clear_graph, test_base):
        """Test end-to-end integration: OGM -> dataset -> Neo4j"""
        class Person(NodeModel):
            _labels = ['Person']
            _merge_keys = ['email']
            name: str
            email: str
            age: int
            
            knows: Relationship = Relationship('Person', 'KNOWS', 'Person')
        
        # Create nodes first
        people = Person.dataset()
        alice = Person(name='Alice', email='alice@example.com', age=30)
        bob = Person(name='Bob', email='bob@example.com', age=25)
        charlie = Person(name='Charlie', email='charlie@example.com', age=35)
        
        people.add(alice)
        people.add(bob)
        people.add(charlie)
        people.create(graph)
        
        # Create relationships using dataset
        friendships = Person.knows.dataset()
        friendships.add(alice, bob, {'since': '2020'})
        friendships.add(bob, charlie, {'since': '2021'})
        friendships.add(alice, charlie, {'since': '2019'})
        
        friendships.create(graph)
        
        # Verify in Neo4j
        result = run_query_return_results(graph, "MATCH ()-[r:KNOWS]->() RETURN count(r)")
        assert result[0][0] == 3
        
        # Verify relationship properties
        result = run_query_return_results(graph,
            "MATCH (a:Person {email: 'alice@example.com'})-[r:KNOWS]->(b:Person {email: 'bob@example.com'}) RETURN r.since")
        assert result[0][0] == '2020'


class TestRelationshipDescriptor:
    """
    Tests for Relationship descriptor behavior.

    Relationship fields must work as descriptors:
    - Class-level access (Person.knows) returns the Relationship with dataset() method
    - Instance-level access (person.knows) returns a Relationship bound to that instance

    This is critical for Pydantic 2.12+ compatibility where field defaults are
    stored in FieldInfo.default rather than on the class directly.
    """

    def test_class_level_relationship_has_dataset(self, test_base):
        """Class-level Relationship access should have dataset() method."""
        class Person(NodeModel):
            name: str

            _labels = ['Person']
            _merge_keys = ['name']

            knows: Relationship = Relationship('Person', 'KNOWS', 'Person')

        # Class-level access should return Relationship with dataset()
        assert hasattr(Person.knows, 'dataset')
        dataset = Person.knows.dataset()
        assert dataset is not None

    def test_instance_relationship_has_parent(self, test_base):
        """Instance-level Relationship should have _parent_instance set."""
        class Person(NodeModel):
            name: str

            _labels = ['Person']
            _merge_keys = ['name']

            knows: Relationship = Relationship('Person', 'KNOWS', 'Person')

        alice = Person(name='Alice')

        # Instance-level access should have _parent_instance pointing to the instance
        assert alice.knows._parent_instance is alice

    def test_separate_instances_have_separate_relationships(self, test_base):
        """Each instance should have its own Relationship with correct parent."""
        class Person(NodeModel):
            name: str

            _labels = ['Person']
            _merge_keys = ['name']

            knows: Relationship = Relationship('Person', 'KNOWS', 'Person')

        alice = Person(name='Alice')
        bob = Person(name='Bob')

        # Each instance has its own relationship
        assert alice.knows._parent_instance is alice
        assert bob.knows._parent_instance is bob
        assert alice.knows is not bob.knows


class TestPydanticCompatibility:
    """
    Tests to verify that NodeModel works correctly as a Pydantic model.

    NodeModel extends Pydantic's BaseModel, so standard Pydantic features
    like field validation, custom validators, and serialization should work.
    """

    def test_field_constraints(self, test_base):
        """Field() constraints like ge, le, min_length should work."""
        from pydantic import Field, ValidationError

        class Person(NodeModel):
            name: str = Field(min_length=1)
            age: int = Field(ge=0, le=150)

            _labels = ['Person']
            _merge_keys = ['name']

        # Valid data should work
        p = Person(name='Alice', age=30)
        assert p.name == 'Alice'
        assert p.age == 30

        # Invalid age (too high) should fail
        with pytest.raises(ValidationError):
            Person(name='Bob', age=200)

        # Invalid age (negative) should fail
        with pytest.raises(ValidationError):
            Person(name='Charlie', age=-1)

    def test_custom_validator(self, test_base):
        """Custom field validators should work."""
        from pydantic import field_validator, ValidationError

        class Person(NodeModel):
            name: str
            email: str

            _labels = ['Person']
            _merge_keys = ['name']

            @field_validator('email')
            @classmethod
            def email_must_contain_at(cls, v):
                if '@' not in v:
                    raise ValueError('email must contain @')
                return v

        # Valid email should work
        p = Person(name='Alice', email='alice@example.com')
        assert p.email == 'alice@example.com'

        # Invalid email should fail
        with pytest.raises(ValidationError):
            Person(name='Bob', email='invalid-email')

    def test_model_dump(self, test_base):
        """model_dump() should work and return node properties."""
        class Person(NodeModel):
            name: str
            age: int

            _labels = ['Person']
            _merge_keys = ['name']

            knows: Relationship = Relationship('Person', 'KNOWS', 'Person')

        p = Person(name='Alice', age=30)
        dump = p.model_dump()

        # Should contain the basic fields
        assert dump['name'] == 'Alice'
        assert dump['age'] == 30
        # Note: 'knows' will be included in model_dump() but filtered out
        # by _all_properties when adding to NodeSet

    def test_model_copy(self, test_base):
        """model_copy() should create a copy with updated fields."""
        class Person(NodeModel):
            name: str
            age: int

            _labels = ['Person']
            _merge_keys = ['name']

        p = Person(name='Alice', age=30)
        p_copy = p.model_copy(update={'age': 31})

        # Original unchanged
        assert p.age == 30
        # Copy has new value
        assert p_copy.age == 31
        assert p_copy.name == 'Alice'

    def test_all_properties_excludes_relationships(self, test_base):
        """_all_properties should exclude Relationship fields for Neo4j storage."""
        class Person(NodeModel):
            name: str
            age: int

            _labels = ['Person']
            _merge_keys = ['name']

            knows: Relationship = Relationship('Person', 'KNOWS', 'Person')

        p = Person(name='Alice', age=30)
        props = p._all_properties

        # Should have regular fields
        assert props['name'] == 'Alice'
        assert props['age'] == 30
        # Should NOT have Relationship
        assert 'knows' not in props


class TestBaseClassVars:
    """Test that Base class variables are proper ClassVars, not PrivateAttr.

    On Python 3.14 with Pydantic 2.12+, underscore-prefixed attributes like
    `_database = None` are automatically treated as PrivateAttr. This causes
    issues when accessing them at the class level - they return the descriptor
    object instead of None.

    These tests ensure _driver and _database are properly typed as ClassVar
    to prevent this issue.
    """

    def test_database_is_not_private_attr(self, test_base):
        """Test that Base._database is None, not a PrivateAttr descriptor."""
        from pydantic.fields import ModelPrivateAttr
        from neo4j import DEFAULT_DATABASE

        # Reset to ensure clean state
        test_base.set_database(None)

        # _database should be None, not a ModelPrivateAttr
        assert test_base._database is None
        assert not isinstance(test_base._database, ModelPrivateAttr)

        # get_database() should return DEFAULT_DATABASE when _database is None
        assert test_base.get_database() == DEFAULT_DATABASE

    def test_driver_is_not_private_attr(self, test_base):
        """Test that Base._driver is a Driver or None, not a PrivateAttr descriptor."""
        from pydantic.fields import ModelPrivateAttr

        # _driver should be a Driver instance (set by fixture), not ModelPrivateAttr
        assert not isinstance(test_base._driver, ModelPrivateAttr)
        # Should be able to call get_driver() without error
        driver = test_base.get_driver()
        assert driver is not None

    def test_set_and_get_database(self, test_base):
        """Test that set_database/get_database work correctly."""
        from neo4j import DEFAULT_DATABASE

        # Set a custom database
        test_base.set_database("testdb")
        assert test_base.get_database() == "testdb"

        # Reset to None - should fall back to DEFAULT_DATABASE
        test_base.set_database(None)
        assert test_base.get_database() == DEFAULT_DATABASE

    def test_database_and_driver_are_classvars(self):
        """Test that Base._database and _driver are typed as ClassVar.

        This test catches a bug where on Python 3.14 with Pydantic 2.12+,
        underscore-prefixed class attributes like `_database = None` become
        ModelPrivateAttr descriptors if not annotated as ClassVar.

        Without ClassVar, accessing Base._database before any set_database()
        call returns the descriptor object (truthy) instead of None, causing
        get_database() to return the descriptor instead of DEFAULT_DATABASE.

        By checking the annotations include ClassVar, we ensure the fix is
        in place without needing to reload modules.
        """
        from typing import ClassVar, get_origin
        from graphio.ogm.model import Base

        # Check that _database is annotated as ClassVar
        assert '_database' in Base.__annotations__, \
            "_database should be in Base.__annotations__"
        db_annotation = Base.__annotations__['_database']
        assert get_origin(db_annotation) is ClassVar, \
            f"_database should be ClassVar, got {db_annotation}"

        # Check that _driver is annotated as ClassVar
        assert '_driver' in Base.__annotations__, \
            "_driver should be in Base.__annotations__"
        driver_annotation = Base.__annotations__['_driver']
        assert get_origin(driver_annotation) is ClassVar, \
            f"_driver should be ClassVar, got {driver_annotation}"