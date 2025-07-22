# Getting Started: OGM Track

**Best for**: Applications with complex data models, interactive queries, and relationship traversals.

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
from graphio import NodeModel, Relationship, Base
from neo4j import GraphDatabase

# Connect to Neo4j
driver = GraphDatabase.driver('neo4j://localhost:7687', auth=('neo4j', 'password'))
Base.set_driver(driver)
```

## Step 2: Define Your Models

```python
class Person(NodeModel):
    _labels = ['Person']
    _merge_keys = ['email']  # Unique identifier
    
    name: str
    email: str
    age: int
    
    # Define relationships
    works_at: Relationship = Relationship('Person', 'WORKS_AT', 'Company')

class Company(NodeModel):
    _labels = ['Company']
    _merge_keys = ['name']
    
    name: str
    industry: str
```

## Step 3: Create and Query Data

```python
# Create companies
acme = Company(name='ACME Corp', industry='Technology')
acme.merge()

# Create people and relationships
alice = Person(name='Alice Smith', email='alice@example.com', age=30)
alice.works_at.add(acme, {'position': 'Developer', 'since': '2023'})
alice.merge()

# Query data
developers = Person.match(Person.age > 25).all()
alice_company = alice.works_at.match().first()
```

## Step 4: Create Indexes (Performance)

```python
# Create indexes for all models
Base.create_indexes()
```

## What You've Learned

✅ How to define Pydantic-based models with Graphio  
✅ How to create nodes and relationships  
✅ How to query data using intuitive syntax  
✅ How to optimize with indexes  

## Next Steps

- **Ready for bulk data?** → [Bulk Loading Track](bulk.md)
- **Want to combine both?** → [Hybrid Approach](hybrid.md)
- **Deep dive into OGM** → [Complete OGM Guide](../details/ogm.md)