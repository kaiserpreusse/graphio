# Graphio

**Graphio** makes loading data into Neo4j fast and intuitive. Use the Pydantic-based **Object Graph Mapper** for complex applications and **bulk loading** for high-performance data ingestion — or combine both approaches in the same project for maximum flexibility.

## Quick Example

=== "OGM (Object-Oriented)"

    ```python
    from graphio import NodeModel, Relationship, Base
    from neo4j import GraphDatabase
    
    # Set up connection
    driver = GraphDatabase.driver('neo4j://localhost:7687', auth=('neo4j', 'password'))
    Base.set_driver(driver)
    
    # Define models with relationships
    class Person(NodeModel):
        _labels = ['Person']
        _merge_keys = ['name']
        
        name: str
        age: int
        
        friends: Relationship = Relationship('Person', 'FRIENDS_WITH', 'Person')
        works_at: Relationship = Relationship('Person', 'WORKS_AT', 'Company')

    class Company(NodeModel):
        _labels = ['Company']
        _merge_keys = ['name']
        
        name: str
        industry: str
        
        # Same relationship - query from either direction
        employees: Relationship = Relationship('Person', 'WORKS_AT', 'Company')
    
    # Use it
    alice = Person(name='Alice', age=30)
    acme = Company(name='ACME Corp', industry='Technology')
    alice.works_at.add(acme)
    alice.merge()
    
    # Query from either side
    alice_company = alice.works_at.match().first()
    acme_employees = acme.employees.match().all()
    ```

=== "Bulk Loading (High Performance)"

    ```python  
    from graphio import NodeSet, RelationshipSet
    from neo4j import GraphDatabase
    
    # Set up connection
    driver = GraphDatabase.driver('neo4j://localhost:7687', auth=('neo4j', 'password'))
    
    # Define data containers
    people = NodeSet(['Person'], merge_keys=['name'])
    friendships = RelationshipSet('FRIENDS_WITH', ['Person'], ['Person'], ['name'], ['name'])
    
    # Add data
    people.add({'name': 'Alice', 'age': 30})
    people.add({'name': 'Bob', 'age': 25})
    friendships.add({'name': 'Alice'}, {'name': 'Bob'})
    
    # Bulk load to Neo4j
    people.create(driver)
    friendships.create(driver)
    ```

=== "Hybrid Approach (Best of Both)"

    ```python
    from graphio import NodeModel, Base
    from neo4j import GraphDatabase
    
    # Set up connection
    driver = GraphDatabase.driver('neo4j://localhost:7687', auth=('neo4j', 'password'))
    Base.set_driver(driver)
    
    # Define OGM model for structure and validation
    class Person(NodeModel):
        _labels = ['Person']
        _merge_keys = ['email']
        name: str
        email: str
    
    # Get bulk container directly from OGM model
    people = Person.dataset()  # Automatically uses Person's labels and merge_keys
    
    for person_data in large_dataset:
        # Create validated OGM instance and add directly
        person = Person(**person_data)  # Pydantic validation happens here
        people.add(person)  # Add validated instance to bulk dataset
    
    people.create(driver)  # Bulk create with validation benefits
    
    # Use OGM for application logic
    alice = Person.match(Person.email == 'alice@example.com').first()
    ```

## Install

```bash
pip install graphio
```

## What's Next?

- **New to Graphio?** → [Getting Started](getting_started/index.md) - Learn both approaches
- **Need comprehensive docs?** → [User Guide](details/ogm.md) - Deep dive into OGM and bulk loading
- **API reference?** → [API Reference](api_reference/nodeset.md) - Complete method reference

!!! tip "Pro Tip"
    Most production applications use **both approaches**: OGM for application logic and bulk loading for data ingestion. They complement each other perfectly!