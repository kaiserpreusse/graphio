import pytest
from graphio.bulk.nodeset import NodeSet
from graphio import NodeModel
from graphio.utils import run_query_return_results


class TestNodeSetDeduplication:
    """Test NodeSet deduplication functionality"""
    
    def test_deduplication_disabled_by_default(self):
        """Test that deduplication is disabled by default"""
        ns = NodeSet(['Person'], merge_keys=['name'])
        
        # Add duplicate nodes
        ns.add_node({'name': 'Alice', 'age': 30})
        ns.add_node({'name': 'Alice', 'age': 31})
        ns.add_node({'name': 'Bob', 'age': 25})
        
        assert len(ns.nodes) == 3
        assert ns.deduplicate is False
        assert ns._merge_key_index is None
    
    def test_deduplication_enabled(self):
        """Test basic deduplication functionality"""
        ns = NodeSet(['Person'], merge_keys=['name'], deduplicate=True)
        
        # Add nodes with duplicate merge keys
        ns.add_node({'name': 'Alice', 'age': 30})
        ns.add_node({'name': 'Alice', 'age': 31})  # Should be skipped
        ns.add_node({'name': 'Bob', 'age': 25})
        ns.add_node({'name': 'Charlie', 'age': 35})
        ns.add_node({'name': 'Bob', 'age': 26})  # Should be skipped
        
        assert len(ns.nodes) == 3
        assert ns.deduplicate is True
        assert ns._merge_key_index is not None
        assert len(ns._merge_key_index) == 3
        
        # Verify the actual nodes
        names = [n['name'] for n in ns.nodes]
        assert names == ['Alice', 'Bob', 'Charlie']
        
        # Verify ages - should keep first occurrence
        alice_node = [n for n in ns.nodes if n['name'] == 'Alice'][0]
        assert alice_node['age'] == 30
    
    def test_deduplication_with_force_parameter(self):
        """Test force parameter overrides deduplication"""
        ns = NodeSet(['Person'], merge_keys=['name'], deduplicate=True)
        
        # Add initial nodes
        ns.add_node({'name': 'Alice', 'age': 30})
        ns.add_node({'name': 'Bob', 'age': 25})
        
        # Try to add duplicate without force (should be skipped)
        ns.add_node({'name': 'Alice', 'age': 31})
        assert len(ns.nodes) == 2
        
        # Add duplicate with force=True (should be added)
        ns.add_node({'name': 'Alice', 'age': 32}, force=True)
        assert len(ns.nodes) == 3
        
        # Verify we have two Alice nodes
        alice_nodes = [n for n in ns.nodes if n['name'] == 'Alice']
        assert len(alice_nodes) == 2
        assert alice_nodes[0]['age'] == 30
        assert alice_nodes[1]['age'] == 32
    
    def test_deduplication_multiple_merge_keys(self):
        """Test deduplication with multiple merge keys"""
        ns = NodeSet(['Person'], merge_keys=['name', 'city'], deduplicate=True)
        
        # Add nodes - same name but different city should be allowed
        ns.add_node({'name': 'Alice', 'city': 'NYC', 'age': 30})
        ns.add_node({'name': 'Alice', 'city': 'LA', 'age': 31})
        ns.add_node({'name': 'Alice', 'city': 'NYC', 'age': 32})  # Should be skipped
        ns.add_node({'name': 'Bob', 'city': 'NYC', 'age': 25})
        
        assert len(ns.nodes) == 3
        
        # Verify the nodes
        nyc_alice = [n for n in ns.nodes if n['name'] == 'Alice' and n['city'] == 'NYC'][0]
        la_alice = [n for n in ns.nodes if n['name'] == 'Alice' and n['city'] == 'LA'][0]
        assert nyc_alice['age'] == 30  # First one should be kept
        assert la_alice['age'] == 31
    
    def test_add_method_alias_with_deduplication(self):
        """Test that add() method alias works with deduplication"""
        ns = NodeSet(['Person'], merge_keys=['name'], deduplicate=True)
        
        # Use add() instead of add_node()
        ns.add({'name': 'Alice', 'age': 30})
        ns.add({'name': 'Alice', 'age': 31})  # Should be skipped
        ns.add({'name': 'Bob', 'age': 25})
        
        assert len(ns.nodes) == 2
        
        # Test force parameter with add()
        ns.add({'name': 'Alice', 'age': 32}, force=True)
        assert len(ns.nodes) == 3
    
    def test_add_nodes_batch_with_deduplication(self):
        """Test add_nodes() batch method with deduplication"""
        ns = NodeSet(['Person'], merge_keys=['name'], deduplicate=True)
        
        # Add batch of nodes with duplicates
        nodes = [
            {'name': 'Alice', 'age': 30},
            {'name': 'Bob', 'age': 25},
            {'name': 'Alice', 'age': 31},  # Should be skipped
            {'name': 'Charlie', 'age': 35},
            {'name': 'Bob', 'age': 26},  # Should be skipped
        ]
        ns.add_nodes(nodes)
        
        assert len(ns.nodes) == 3
        names = [n['name'] for n in ns.nodes]
        assert set(names) == {'Alice', 'Bob', 'Charlie'}
        
        # Test force parameter with add_nodes
        force_nodes = [
            {'name': 'Alice', 'age': 40},
            {'name': 'David', 'age': 45}
        ]
        ns.add_nodes(force_nodes, force=True)
        
        assert len(ns.nodes) == 5  # 3 original + 2 forced
    
    def test_deduplication_with_default_props(self):
        """Test deduplication works correctly with default props"""
        ns = NodeSet(
            ['Person'], 
            merge_keys=['name'], 
            default_props={'status': 'active'},
            deduplicate=True
        )
        
        # Add nodes - default props should be applied before deduplication check
        ns.add_node({'name': 'Alice', 'age': 30})
        ns.add_node({'name': 'Alice', 'age': 31})  # Should be skipped
        
        assert len(ns.nodes) == 1
        assert ns.nodes[0]['status'] == 'active'
        assert ns.nodes[0]['age'] == 30
    
    def test_deduplication_empty_merge_keys(self):
        """Test behavior when merge_keys is None or empty"""
        # This should work but deduplication won't be meaningful
        ns1 = NodeSet(['Person'], merge_keys=None, deduplicate=True)
        ns2 = NodeSet(['Person'], merge_keys=[], deduplicate=True)
        
        # Both should initialize without error
        assert ns1.deduplicate is True
        assert ns2.deduplicate is True
    
    def test_deduplication_index_persistence(self):
        """Test that deduplication index persists across operations"""
        ns = NodeSet(['Person'], merge_keys=['name'], deduplicate=True)
        
        # Add initial nodes
        ns.add_node({'name': 'Alice', 'age': 30})
        ns.add_node({'name': 'Bob', 'age': 25})
        
        # Check index
        assert len(ns._merge_key_index) == 2
        assert ('Alice',) in ns._merge_key_index
        assert ('Bob',) in ns._merge_key_index
        
        # Add more nodes (some duplicates)
        ns.add_node({'name': 'Alice', 'age': 31})  # Should be skipped
        ns.add_node({'name': 'Charlie', 'age': 35})
        
        # Index should have 3 entries
        assert len(ns._merge_key_index) == 3
        assert ('Charlie',) in ns._merge_key_index
        
        # Force add a duplicate
        ns.add_node({'name': 'Alice', 'age': 32}, force=True)
        
        # Index should still have 3 entries (force doesn't update index)
        assert len(ns._merge_key_index) == 3
        assert len(ns.nodes) == 4  # 2 Alice, 1 Bob, 1 Charlie
    
    def test_deduplication_with_ogm_instances(self, test_base):
        """Test deduplication works with OGM model instances"""
        class Person(NodeModel):
            name: str
            age: int
            _labels = ['Person']
            _merge_keys = ['name']
        
        ns = NodeSet(['Person'], merge_keys=['name'], deduplicate=True)
        
        # Add OGM instances
        p1 = Person(name='Alice', age=30)
        p2 = Person(name='Alice', age=31)  # Should be skipped
        p3 = Person(name='Bob', age=25)
        
        ns.add_node(p1)
        ns.add_node(p2)
        ns.add_node(p3)
        
        assert len(ns.nodes) == 2
        names = [n['name'] for n in ns.nodes]
        assert set(names) == {'Alice', 'Bob'}
    
    def test_deduplication_mixed_types(self, test_base):
        """Test deduplication with mixed dict and OGM instances"""
        class Person(NodeModel):
            name: str
            age: int
            _labels = ['Person']
            _merge_keys = ['name']
        
        ns = NodeSet(['Person'], merge_keys=['name'], deduplicate=True)
        
        # Add mixed types
        ns.add_node({'name': 'Alice', 'age': 30})  # dict
        ns.add_node(Person(name='Alice', age=31))  # OGM - should be skipped
        ns.add_node({'name': 'Bob', 'age': 25})    # dict
        ns.add_node(Person(name='Charlie', age=35))  # OGM
        
        assert len(ns.nodes) == 3
        names = [n['name'] for n in ns.nodes]
        assert set(names) == {'Alice', 'Bob', 'Charlie'}


class TestNodeSetDeduplicationIntegration:
    """Integration tests with Neo4j database"""
    
    def test_deduplication_create_operation(self, graph, clear_graph):
        """Test that deduplicated NodeSet creates correct number of nodes"""
        ns = NodeSet(['Person'], merge_keys=['name'], deduplicate=True)
        
        # Add nodes with duplicates
        for i in range(5):
            ns.add_node({'name': 'Alice', 'age': 30 + i})
            ns.add_node({'name': 'Bob', 'age': 25 + i})
        
        # Should only have 2 unique nodes
        assert len(ns.nodes) == 2
        
        # Create in database
        ns.create(graph)
        
        # Verify in database
        result = run_query_return_results(graph, "MATCH (n:Person) RETURN count(n)")[0][0]
        assert result == 2
        
        # Verify the specific nodes
        alice_result = run_query_return_results(
            graph, 
            "MATCH (n:Person {name: 'Alice'}) RETURN n.age"
        )[0][0]
        assert alice_result == 30  # First occurrence
    
    def test_deduplication_merge_operation(self, graph, clear_graph):
        """Test that deduplicated NodeSet merges correctly"""
        # First create some nodes
        initial_ns = NodeSet(['Person'], merge_keys=['name'])
        initial_ns.add_node({'name': 'Alice', 'age': 30})
        initial_ns.add_node({'name': 'Bob', 'age': 25})
        initial_ns.merge(graph)
        
        # Now merge with deduplication
        ns = NodeSet(['Person'], merge_keys=['name'], deduplicate=True)
        
        # Add nodes - some existing, some new, with duplicates
        ns.add_node({'name': 'Alice', 'age': 31})  # Existing
        ns.add_node({'name': 'Alice', 'age': 32})  # Duplicate - should be skipped
        ns.add_node({'name': 'Bob', 'age': 26})    # Existing
        ns.add_node({'name': 'Charlie', 'age': 35})  # New
        ns.add_node({'name': 'Charlie', 'age': 36})  # Duplicate - should be skipped
        
        assert len(ns.nodes) == 3
        
        # Merge to database
        ns.merge(graph)
        
        # Should have 3 total nodes
        result = run_query_return_results(graph, "MATCH (n:Person) RETURN count(n)")[0][0]
        assert result == 3
        
        # Check that Alice was updated to age 31 (not 32)
        alice_result = run_query_return_results(
            graph, 
            "MATCH (n:Person {name: 'Alice'}) RETURN n.age"
        )[0][0]
        assert alice_result == 31
    
    def test_deduplication_performance_benefit(self):
        """Test that deduplication provides performance benefit for large datasets"""
        # This is more of a demonstration than a strict test
        ns_dedup = NodeSet(['Person'], merge_keys=['name', 'email'], deduplicate=True)
        ns_no_dedup = NodeSet(['Person'], merge_keys=['name', 'email'])
        
        # Simulate data with many duplicates
        for i in range(1000):
            # Add same 10 people repeatedly
            for j in range(10):
                person = {
                    'name': f'Person{j}',
                    'email': f'person{j}@example.com',
                    'iteration': i
                }
                ns_dedup.add_node(person)
                ns_no_dedup.add_node(person)
        
        # With deduplication: should have only 10 unique nodes
        assert len(ns_dedup.nodes) == 10
        # Without deduplication: should have all 10,000 nodes
        assert len(ns_no_dedup.nodes) == 10000
        
        # The deduplicated set will be much faster to merge/create in Neo4j