# Getting Started: Bulk Loading Track

**Best for**: ETL processes, large datasets, data migration, and high-performance data ingestion.

## Prerequisites

1. **Neo4j Database**: Running locally or remotely
   ```bash
   # Using Docker (recommended for testing)
   docker run -p 7474:7474 -p 7687:7687 -e NEO4J_AUTH=neo4j/password neo4j:latest
   ```

2. **Install Graphio**:
   ```bash
   pip install graphio
   ```

## Step 1: Set Up Connection

```python
from graphio import NodeSet, RelationshipSet
from neo4j import GraphDatabase

# Connect to Neo4j
driver = GraphDatabase.driver('neo4j://localhost:7687', auth=('neo4j', 'password'))
```

## Step 2: Define Data Containers

```python
# Define node containers
people = NodeSet(['Person'], merge_keys=['email'])
companies = NodeSet(['Company'], merge_keys=['name'])

# Define relationship container
employments = RelationshipSet(
    'WORKS_AT',           # Relationship type
    ['Person'],           # Start node labels  
    ['Company'],          # End node labels
    ['email'],            # Start node match keys
    ['name']              # End node match keys
)
```

## Step 3: Add Data in Batches

```python
# Add nodes (can handle thousands efficiently)
people.add_node({'name': 'Alice Smith', 'email': 'alice@example.com', 'age': 30})
people.add_node({'name': 'Bob Johnson', 'email': 'bob@example.com', 'age': 25})

companies.add_node({'name': 'ACME Corp', 'industry': 'Technology'})

# Add relationships
employments.add_relationship(
    {'email': 'alice@example.com'},  # Start node
    {'name': 'ACME Corp'},           # End node  
    {'position': 'Developer'}        # Relationship properties
)
```

## Step 4: Create Indexes (Performance)

```python
# Create indexes before bulk loading
people.create_index(driver)
companies.create_index(driver)
```

## Step 5: Bulk Load to Neo4j

```python
# Load data efficiently
companies.create(driver)  # Load companies first
people.create(driver)     # Then people
employments.create(driver)  # Finally relationships

print(f"Loaded {len(people.nodes)} people and {len(companies.nodes)} companies")
```

## What You've Learned

✅ How to create NodeSet and RelationshipSet containers  
✅ How to batch data for efficient loading  
✅ How to create indexes for performance  
✅ Proper loading order (nodes before relationships)  

## Next Steps

- **Need data validation?** → [OGM Track](ogm.md)
- **Want to combine both?** → [Hybrid Approach](hybrid.md)
- **Deep dive into bulk loading** → [Bulk Loading Guide](../details/bulk.md)