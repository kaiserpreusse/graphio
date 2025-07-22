"""
Integration tests combining OGM and bulk operations.

These tests simulate real-world usage patterns where users mix OGM models
with bulk loading operations, testing the interaction between components.
"""
import pytest
from graphio import NodeModel, Relationship, NodeSet, RelationshipSet
from graphio.utils import run_query_return_results


class TestOGMBulkIntegration:
    """Test scenarios mixing OGM models with bulk operations."""

    def test_bulk_load_then_ogm_query(self, test_base):
        """Load data with bulk operations, then query with OGM."""
        
        # Define models for this test
        class Person(NodeModel):
            name: str
            age: int
            email: str
            department: str = None
            _labels = ['Person']
            _merge_keys = ['email']

            works_for: Relationship = Relationship('Person', 'WORKS_FOR', 'Company')

        class Company(NodeModel):
            name: str
            industry: str
            founded: int
            _labels = ['Company']
            _merge_keys = ['name']
        
        driver = test_base.get_driver()
        
        # 1. Bulk load companies
        companies = NodeSet(labels=['Company'], merge_keys=['name'])
        companies.add({'name': 'TechCorp', 'industry': 'Technology', 'founded': 2010})
        companies.add({'name': 'FinanceInc', 'industry': 'Finance', 'founded': 2015})
        companies.create(driver)
        
        # 2. Bulk load people
        people = NodeSet(labels=['Person'], merge_keys=['email'])
        people.add({'name': 'Alice', 'age': 30, 'email': 'alice@techcorp.com', 'department': 'Engineering'})
        people.add({'name': 'Bob', 'age': 25, 'email': 'bob@financeinc.com', 'department': 'Sales'})
        people.create(driver)
        
        # 3. Bulk load relationships
        work_rels = RelationshipSet(
            rel_type='WORKS_FOR',
            start_node_labels=['Person'], 
            end_node_labels=['Company'],
            start_node_properties=['email'],
            end_node_properties=['name']
        )
        work_rels.add(
            {'email': 'alice@techcorp.com'}, 
            {'name': 'TechCorp'}, 
            {'start_date': '2020-01-01', 'role': 'Senior Developer'}
        )
        work_rels.add(
            {'email': 'bob@financeinc.com'}, 
            {'name': 'FinanceInc'}, 
            {'start_date': '2021-06-01', 'role': 'Account Manager'}
        )
        work_rels.create(driver)
        
        # 4. Now use OGM to query the bulk-loaded data
        all_people = Person.match().all()
        assert len(all_people) == 2
        
        # Find Alice and verify her company relationship
        alice = Person.match(Person.name == 'Alice').first()
        assert alice is not None
        assert alice.name == 'Alice'
        assert alice.department == 'Engineering'
        
        # Query relationships through OGM
        alice_companies = alice.works_for.match().all()
        assert len(alice_companies) == 1
        assert alice_companies[0].name == 'TechCorp'
        
        # Test company queries
        tech_companies = Company.match(Company.industry == 'Technology').all()
        assert len(tech_companies) == 1
        assert tech_companies[0].name == 'TechCorp'

    def test_ogm_create_then_bulk_extend(self, test_base):
        """Create initial data with OGM, then extend with bulk operations."""
        
        # Define models for this test
        class Person(NodeModel):
            name: str
            age: int
            email: str
            department: str = None
            _labels = ['Person']
            _merge_keys = ['email']

            works_for: Relationship = Relationship('Person', 'WORKS_FOR', 'Company')

        class Company(NodeModel):
            name: str
            industry: str
            founded: int
            _labels = ['Company']
            _merge_keys = ['name']

            employs: Relationship = Relationship('Company', 'EMPLOYS', 'Person')
        
        driver = test_base.get_driver()
        
        # 1. Create companies using OGM
        techcorp = Company(name='TechCorp', industry='Technology', founded=2010)
        techcorp.create()
        
        startupinc = Company(name='StartupInc', industry='Technology', founded=2020)
        startupinc.create()
        
        # 2. Create some people with OGM
        alice = Person(name='Alice', age=30, email='alice@techcorp.com', department='Engineering')
        alice.works_for.add(techcorp, {'start_date': '2020-01-01', 'role': 'Senior Developer'})
        alice.create()
        
        # 3. Bulk load additional people to extend the dataset
        people_bulk = NodeSet(labels=['Person'], merge_keys=['email'])
        for i in range(10):  # Reduced from 100 to 10 for faster testing
            people_bulk.add({
                'name': f'Employee_{i}',
                'age': 20 + (i % 40),
                'email': f'employee_{i}@techcorp.com',
                'department': 'Engineering' if i % 2 == 0 else 'Marketing'
            })
        people_bulk.create(driver)
        
        # 4. Bulk load relationships for new employees
        work_rels = RelationshipSet(
            rel_type='WORKS_FOR',
            start_node_labels=['Person'], 
            end_node_labels=['Company'],
            start_node_properties=['email'],
            end_node_properties=['name']
        )
        for i in range(10):
            work_rels.add(
                {'email': f'employee_{i}@techcorp.com'}, 
                {'name': 'TechCorp'}, 
                {'start_date': '2023-01-01', 'role': 'Developer' if i % 2 == 0 else 'Marketer'}
            )
        work_rels.create(driver)
        
        # 5. Verify using OGM queries
        all_people = Person.match().all()
        assert len(all_people) == 11  # Alice + 10 bulk loaded
        
        # Test that OGM can still query the mixed dataset efficiently
        engineers = Person.match(Person.department == 'Engineering').all()
        assert len(engineers) == 6  # Alice + 5 bulk loaded engineers
        
        # Test relationship traversal works with mixed data
        # Since we only created WORKS_FOR relationships, let's test those
        # Count employees by checking WORKS_FOR relationships
        techcorp_loaded = Company.match(Company.name == 'TechCorp').first()
        
        # Alternative approach: query all people who work for TechCorp
        techcorp_employees = Person.match().all()
        techcorp_worker_count = 0
        for person in techcorp_employees:
            companies = person.works_for.match().all()
            if any(c.name == 'TechCorp' for c in companies):
                techcorp_worker_count += 1
        
        assert techcorp_worker_count == 11

    def test_mixed_data_consistency(self, test_base):
        """Test data consistency when mixing OGM and bulk operations."""
        
        # Define models for this test
        class Person(NodeModel):
            name: str
            age: int
            email: str
            department: str = None
            _labels = ['Person']
            _merge_keys = ['email']

            works_for: Relationship = Relationship('Person', 'WORKS_FOR', 'Company')

        class Company(NodeModel):
            name: str
            industry: str
            founded: int
            _labels = ['Company']
            _merge_keys = ['name']
        
        driver = test_base.get_driver()
        
        # 1. Create person with OGM
        alice = Person(name='Alice', age=30, email='alice@example.com')
        alice.create()
        
        # 2. Update same person with bulk merge (should not duplicate)
        people_bulk = NodeSet(labels=['Person'], merge_keys=['email'])
        people_bulk.add({
            'name': 'Alice Smith',  # Updated name
            'age': 31,              # Updated age
            'email': 'alice@example.com',  # Same merge key
            'department': 'Engineering'     # New field
        })
        people_bulk.merge(driver)
        
        # 3. Verify only one person exists with updated data
        all_alices = Person.match(Person.email == 'alice@example.com').all()
        assert len(all_alices) == 1
        
        alice_updated = all_alices[0]
        assert alice_updated.name == 'Alice Smith'  # Bulk update won
        assert alice_updated.age == 31
        assert alice_updated.department == 'Engineering'
        
        # 4. Add relationship via OGM after bulk update
        company = Company(name='TechCorp', industry='Technology', founded=2010)
        company.create()
        
        alice_updated.works_for.add(company, {'role': 'Senior Developer'})
        alice_updated.create_relationships()
        
        # 5. Verify relationship exists
        alice_fresh = Person.match(Person.email == 'alice@example.com').first()
        companies = alice_fresh.works_for.match().all()
        assert len(companies) == 1
        assert companies[0].name == 'TechCorp'


class TestErrorRecovery:
    """Test error handling in mixed operations."""

    def test_partial_failure_recovery(self, test_base):
        """Test behavior when some operations succeed and others fail."""
        
        # Define models for this test
        class Person(NodeModel):
            name: str
            age: int
            email: str
            _labels = ['Person']
            _merge_keys = ['email']

        class Company(NodeModel):
            name: str
            industry: str
            founded: int
            _labels = ['Company']
            _merge_keys = ['name']
        
        driver = test_base.get_driver()
        
        # 1. Create valid company
        company = Company(name='ValidCorp', industry='Technology', founded=2010)
        company.create()
        
        # 2. Try to create person with invalid data (missing required field)
        with pytest.raises(Exception):  # Should fail validation
            invalid_person = Person(name='Invalid', age=25)  # Missing required email
            invalid_person.create()
        
        # 3. Verify company still exists despite person failure
        companies = Company.match().all()
        assert len(companies) == 1
        assert companies[0].name == 'ValidCorp'
        
        # 4. Create valid person after failure
        valid_person = Person(name='Valid', age=25, email='valid@example.com')
        valid_person.create()
        
        # 5. Verify everything is consistent
        people = Person.match().all()
        assert len(people) == 1
        assert people[0].name == 'Valid'