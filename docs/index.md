# Graphio

**Graphio** is a Python library designed for efficient interaction with Neo4j.
It provides a simple **Object Graph Mapper (OGM)** to work with
individual nodes and relationships and **simplifies bulk data loading**.

The graphio OGM is based on Pydantic and provides a simple way to define a data
model and use it to load data into Neo4j.
Bulk data loading is facilitated by the `NodeSet` and `RelationshipSet` classes,
which organize nodes and relationships for streamlined data ingestion.

Graphio also supports serializing data to JSON and CSV formats. This feature
is particularly useful for debugging or
saving graph-ready datasets for future use.

Graphio relies on the official Neo4j Python driver for its connection to Neo4j,
ensuring compatibility and performance.

## Install

```shell
pip install graphio
```

## tl;dr

```python
from graphio import NodeModel, Relationship, Base
from neo4j import GraphDatabase

driver = GraphDatabase.driver('neo4j://localhost:7687',
                              auth=('neo4j', 'password'))

Base.set_driver(driver)

class Person(NodeModel):
    # Graphio specific declarations
    _labels = ['Person']
    _merge_keys = ['name']
    
    # Pydantic model declarations
    name: str
    age: int
    
    # Relationships
    likes: Relationship = Relationship('Person', 'LIKES', 'Person')
            

# Create a person
peter = Person(name='Peter', age=42)
peter.merge()

# Create another Person, make sure they LIKE each other
john = Person(name='John', age=35)
john.likes.add(peter, {'since': 'forever'})
john.merge()

```