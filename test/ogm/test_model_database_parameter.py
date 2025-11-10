"""
Tests for database parameter support in OGM.
These tests require Neo4j Enterprise Edition (multi-database support).
"""

import pytest
from graphio.ogm.model import Base, NodeModel
from graphio.bulk import NodeSet, RelationshipSet


class TestDatabaseParameter:
    """Tests for database parameter - requires Enterprise Edition"""

    def test_base_set_database(self, graph, test_database, skip_if_community):
        """Test Base.set_database() and get_database() methods"""
        Base.set_database(test_database)
        assert Base.get_database() == "graphiotest"

    def test_base_get_database_default(self, graph, skip_if_community):
        """Test Base.get_database() returns default when not set"""
        # Reset database setting
        Base.set_database(None)
        from neo4j import DEFAULT_DATABASE
        assert Base.get_database() == DEFAULT_DATABASE

    def test_node_create_in_database(self, graph, test_database, skip_if_community):
        """Test creating nodes in a specific database"""

        class Person(NodeModel):
            _labels = ['Person']
            _merge_keys = ['name']
            name: str
            age: int

        # Set global database
        Base.set_database(test_database)

        # Create node - should use test_database
        person = Person(name="Alice", age=30)
        person.create()

        # Verify it's in test_database
        with graph.session(database=test_database) as session:
            result = session.run("MATCH (p:Person {name: 'Alice'}) RETURN p.age as age")
            record = result.single()
            assert record is not None
            assert record['age'] == 30

        # Verify it's NOT in default database
        with graph.session(database="neo4j") as session:
            result = session.run("MATCH (p:Person {name: 'Alice'}) RETURN count(p) as count")
            assert result.single()['count'] == 0

    def test_node_merge_in_database(self, graph, test_database, skip_if_community):
        """Test merging nodes in a specific database"""

        class Person(NodeModel):
            _labels = ['Person']
            _merge_keys = ['name']
            name: str
            age: int

        Base.set_database(test_database)

        # First merge
        person1 = Person(name="Bob", age=25)
        person1.merge()

        # Second merge with updated age
        person2 = Person(name="Bob", age=26)
        person2.merge()

        # Verify merge worked correctly
        with graph.session(database=test_database) as session:
            result = session.run("MATCH (p:Person {name: 'Bob'}) RETURN count(p) as count, p.age as age")
            record = result.single()
            assert record['count'] == 1  # Only one node
            assert record['age'] == 26  # Age was updated

    def test_database_parameter_override(self, graph, test_database, skip_if_community):
        """Test that per-operation database parameter overrides global setting"""

        class Person(NodeModel):
            _labels = ['Person']
            _merge_keys = ['name']
            name: str
            age: int

        # Set global database
        Base.set_database(test_database)

        # Create second database
        other_db = "graphiotest2"
        with graph.session(database="system") as session:
            session.run(f"CREATE DATABASE {other_db} IF NOT EXISTS WAIT")

        try:
            # Create in default database
            alice = Person(name="Alice", age=30)
            alice.create()  # Uses test_database

            # Create in other database (override)
            bob = Person(name="Bob", age=25)
            bob.create(database=other_db)  # Overrides to use other_db

            # Verify Alice is only in test_database
            with graph.session(database=test_database) as session:
                result = session.run("MATCH (p:Person) RETURN p.name as name ORDER BY name")
                records = list(result)
                assert len(records) == 1
                assert records[0]['name'] == "Alice"

            # Verify Bob is only in other_db
            with graph.session(database=other_db) as session:
                result = session.run("MATCH (p:Person) RETURN p.name as name ORDER BY name")
                records = list(result)
                assert len(records) == 1
                assert records[0]['name'] == "Bob"

        finally:
            # Cleanup the extra database
            with graph.session(database="system") as session:
                session.run(f"DROP DATABASE {other_db} IF EXISTS WAIT")

    def test_query_match_with_database(self, graph, test_database, skip_if_community):
        """Test that match queries respect database setting"""

        class Person(NodeModel):
            _labels = ['Person']
            _merge_keys = ['name']
            name: str
            age: int

        Base.set_database(test_database)

        # Create test data
        alice = Person(name="Alice", age=30)
        bob = Person(name="Bob", age=25)
        charlie = Person(name="Charlie", age=35)
        alice.create()
        bob.create()
        charlie.create()

        # Query should use test_database automatically
        results = Person.match(Person.age > 26).all()
        assert len(results) == 2
        names = sorted([r.name for r in results])
        assert names == ["Alice", "Charlie"]

        # Verify default database is empty
        with graph.session(database="neo4j") as session:
            result = session.run("MATCH (p:Person) RETURN count(p) as count")
            assert result.single()['count'] == 0

    def test_node_delete_in_database(self, graph, test_database, skip_if_community):
        """Test deleting nodes from a specific database"""

        class Person(NodeModel):
            _labels = ['Person']
            _merge_keys = ['name']
            name: str
            age: int

        Base.set_database(test_database)

        # Create and then delete
        person = Person(name="ToDelete", age=40)
        person.create()

        # Verify it exists
        with graph.session(database=test_database) as session:
            result = session.run("MATCH (p:Person {name: 'ToDelete'}) RETURN count(p) as count")
            assert result.single()['count'] == 1

        # Delete it
        person.delete()

        # Verify it's gone
        with graph.session(database=test_database) as session:
            result = session.run("MATCH (p:Person {name: 'ToDelete'}) RETURN count(p) as count")
            assert result.single()['count'] == 0

    def test_bulk_nodeset_with_database(self, graph, test_database, skip_if_community):
        """Test NodeSet operations with database parameter"""
        Base.set_database(test_database)

        ns = NodeSet(labels=['Person'], merge_keys=['name'])
        ns.add_node({'name': 'Alice', 'age': 30})
        ns.add_node({'name': 'Bob', 'age': 25})
        ns.add_node({'name': 'Charlie', 'age': 35})

        # Create in test database
        ns.create(graph, database=test_database)

        # Verify
        with graph.session(database=test_database) as session:
            result = session.run("MATCH (p:Person) RETURN count(p) as count")
            assert result.single()['count'] == 3

    def test_bulk_nodeset_merge_with_database(self, graph, test_database, skip_if_community):
        """Test NodeSet merge operations with database parameter"""
        Base.set_database(test_database)

        # First create
        ns1 = NodeSet(labels=['Person'], merge_keys=['name'])
        ns1.add_node({'name': 'Alice', 'age': 30})
        ns1.add_node({'name': 'Bob', 'age': 25})
        ns1.create(graph, database=test_database)

        # Then merge with updates
        ns2 = NodeSet(labels=['Person'], merge_keys=['name'])
        ns2.add_node({'name': 'Alice', 'age': 31})  # Update age
        ns2.add_node({'name': 'Charlie', 'age': 35})  # New person
        ns2.merge(graph, database=test_database)

        # Verify merge worked correctly
        with graph.session(database=test_database) as session:
            result = session.run("MATCH (p:Person) RETURN count(p) as count")
            assert result.single()['count'] == 3  # Alice, Bob, Charlie

            result = session.run("MATCH (p:Person {name: 'Alice'}) RETURN p.age as age")
            assert result.single()['age'] == 31  # Age was updated

    def test_bulk_relationshipset_with_database(self, graph, test_database, skip_if_community):
        """Test RelationshipSet operations with database parameter"""
        Base.set_database(test_database)

        # Create nodes first
        ns = NodeSet(labels=['Person'], merge_keys=['name'])
        ns.add_node({'name': 'Alice'})
        ns.add_node({'name': 'Bob'})
        ns.create(graph, database=test_database)

        # Create relationships
        rs = RelationshipSet(
            rel_type='KNOWS',
            start_node_labels=['Person'],
            end_node_labels=['Person'],
            start_node_properties=['name'],
            end_node_properties=['name']
        )
        rs.add_relationship({'name': 'Alice'}, {'name': 'Bob'}, {'since': 2020})
        rs.create(graph, database=test_database)

        # Verify relationship exists
        with graph.session(database=test_database) as session:
            result = session.run(
                "MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'}) "
                "RETURN r.since as since"
            )
            record = result.single()
            assert record is not None
            assert record['since'] == 2020

    def test_index_creation_with_database(self, graph, test_database, skip_if_community):
        """Test that indexes are created in the correct database"""

        class Person(NodeModel):
            _labels = ['Person']
            _merge_keys = ['email']
            name: str
            email: str

        # Set database and create index
        Base.set_database(test_database)
        Person.create_index()

        # Verify index exists in test database
        with graph.session(database=test_database) as session:
            result = session.run("SHOW INDEXES YIELD name, labelsOrTypes, properties")
            indexes = list(result)

            # Check that Person:email index exists
            person_email_indexes = [
                idx for idx in indexes
                if idx.get('labelsOrTypes') and 'Person' in idx['labelsOrTypes']
                and idx.get('properties') and 'email' in idx['properties']
            ]
            assert len(person_email_indexes) > 0, f"No Person:email index found. Indexes: {indexes}"

    def test_model_create_index_with_database(self, graph, test_database, skip_if_community):
        """Test that Base.model_create_index() creates indexes in correct database"""

        class Person(NodeModel):
            _labels = ['Person']
            _merge_keys = ['email']
            name: str
            email: str

        class Company(NodeModel):
            _labels = ['Company']
            _merge_keys = ['name']
            name: str
            industry: str

        # Set database and create all indexes
        Base.set_database(test_database)
        Base.model_create_index()

        # Verify indexes exist in test database
        with graph.session(database=test_database) as session:
            result = session.run("SHOW INDEXES YIELD name, labelsOrTypes, properties")
            indexes = list(result)

            # Check that both model indexes exist
            person_indexes = [
                idx for idx in indexes
                if idx.get('labelsOrTypes') and 'Person' in idx['labelsOrTypes']
                and idx.get('properties') and 'email' in idx['properties']
            ]
            company_indexes = [
                idx for idx in indexes
                if idx.get('labelsOrTypes') and 'Company' in idx['labelsOrTypes']
                and idx.get('properties') and 'name' in idx['properties']
            ]

            assert len(person_indexes) > 0, f"No Person:email index found. Indexes: {indexes}"
            assert len(company_indexes) > 0, f"No Company:name index found. Indexes: {indexes}"
