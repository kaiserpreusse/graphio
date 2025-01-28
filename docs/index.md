# Graphio

**Graphio** is a Python library designed for bulk-loading data into Neo4j. It allows you to collect and manage multiple
sets of nodes and relationships for streamlined data ingestion.

A typical use case is parsing Excel files to quickly create a Neo4j prototype. Note that **Graphio is exclusively for
data loading**; it is not meant for querying or retrieving data from Neo4j.

Graphio also supports serializing data to JSON and CSV formats. This feature is particularly useful for debugging or
saving graph-ready datasets for future use.

The primary interfaces are the `NodeSet` and `RelationshipSet` classes, which organize nodes and relationships with
similar properties. These data sets can then be loaded into Neo4j using `CREATE` or `MERGE` operations.

Graphio relies on the official Neo4j Python driver for its connection to Neo4j, ensuring compatibility and performance.

## Install

```shell
pip install graphio
```

## tl;dr

```python
from graphio import NodeSet, RelationshipSet
from neo4j import GraphDatabase

driver = GraphDatabase.driver('neo4j://localhost:7687', 
                              auth=('neo4j', 'password'))

people = NodeSet(['Person'], merge_keys=['name'])
likes = RelationshipSet('LIKES', ['Person'], ['Person'], ['name'], ['name'])

people.add_node({'name': 'Alice'})
people.add_node({'name': 'Bob'})
likes.add_relationship({'name': 'Alice'}, {'name': 'Bob'})

people.create(driver)
likes.create(driver)
```