# Getting Started

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

## Example

Here's an example workflow and explanation for parsing a csv file, structuring the data for Graphio, and loading the
graph to Neo4j.

### Example CSV file

```csv
   Alice; Matrix,Titanic
   Peter; Matrix,Forrest Gump
   John; Forrest Gump,Titanic
```

### Step 1: Neo4j connection

The official Neo4j Python driver is required to connect to Neo4j, and a Driver instance is always necessary.

```python 
from neo4j import GraphDatabase

driver = GraphDatabase.driver('neo4j://localhost:7687', auth=('neo4j', 'password'))
```

### Step 2: Define the datasets

Create a `NodeSet` for the movies and a `RelationshipSet` for the relationships between the people and movies.

```python
from graphio import NodeSet, RelationshipSet

people = NodeSet(['Person'], merge_keys=['name'])
movies = NodeSet(['Movie'], merge_keys=['title'])
person_likes_movie = RelationshipSet(
    'LIKES', ['Person'], ['Movie'], ['name'], ['title']
)
```

The `merge_keys` parameter is used to define one or more node properties that define the uniqueness of a node for
`MERGE` operations.

### Step 3: Parse the CSV file

```python
with open('people.csv') as my_file:
    for line in my_file:
        # prepare data from the line
        name, titles = line.split(';')
        # split up the movies
        titles = titles.strip().split(',')

        # add one (Person) node per line
        people.add_node({'name': name})

        # add (Movie) nodes and :LIKES relationships
        for title in titles:
            movies.add_node({'title': title})
            person_likes_movie.add_relationship(
                {'name': name}, {'title': title}, {'source': 'my_file'}
            )
```

### Step 4: Load the data

```python
people.create(driver)
movies.create(driver)
person_likes_movie.create(driver)
```

The `driver` object is passed to the `.create()` function.