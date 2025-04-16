# Overview

Graphio can be used in two ways:

1. Use the OGM to define a data model and interact with Neo4j
2. Define `NodeSet` and `RelationshipSet` objects and load them to Neo4j

The [datesets](#use-datasets) are more suitable for quick data loading or testing of new data models
, while the [OGM](#use-the-ogm) is useful for complex data models and applications.



## Use the OGM

With the OGM, you define a data model and use it to load and retrieve data from Neo4j.

From the data model, you can either use instances of the model to 
individual nodes and relationships or create `NodeSet` and `RelationshipSet` 
objects to load larger amounts of data. 


### Example

Here's an example of how to define a data model for a social network.

#### Define data model

```python
from graphio import NodeModel, Relationship, Base
from Neo4j import GraphDatabase

# get the Neo4j driver
driver = GraphDatabase.driver('neo4j://localhost:7687', 
                              auth=('neo4j', 'password'))

# set the driver for the OGM
Base.set_driver(driver)

class Person(NodeModel):
    _labels = ['Person']
    _merge_keys = ['name']
    
    name: str
    
    likes: Relationship = Relationship('Person', 'LIKES', 'Movie')

class Movie(NodeModel):
    labels = ['Movie']
    merge_keys = ['title']
```
Relations are defined as attributes of the node model. The `Relationship` class takes the source 
node label, the relationship type, and the target node label as arguments.

#### Create indexes
You can create indexes for the data model using the `Base` class. This will create indexes for all node models.
```python
Base.model_create_index()
```

#### Create individual nodes and relationships
Instances of the model can be used to create individual nodes and relationships. 


```python

alice = Person(name='Alice')
matrix = Movie(title='Matrix')
alice.likes(matrix)

alice.create()
```


#### Query data

```python
peter = Person.match(Person.name == 'Peter').first()

peters_movies = peter.likes.match().all()

```

## Use datasets

`NodeSet` and `RelationshipSet` objects are used to define the structure of the
data to be loaded into Neo4j. All nodes in a `NodeSet` have the same labels and
the same unique properties (called `merge_keys` in Graphio). All relationships in a
`RelationshipSet` have the same type and the same source and target nodes.

Based on the structure of the data, Graphio can load large numbers of nodes and relationships
efficiently. Graphio takes care of batching and supports `CREATE` and `MERGE` operations
on nodes and relationships.

### Example

Here's an example of how to define a `NodeSet` and a `RelationshipSet` for a social network.

#### Define datasets

```python
from graphio import NodeSet, RelationshipSet
from neo4j import GraphDatabase

driver = GraphDatabase.driver('neo4j://localhost:7687', auth=('neo4j', 'password'))

people = NodeSet(['Person'], merge_keys=['name'])
movies = NodeSet(['Movie'], merge_keys=['title'])
person_likes_movie = RelationshipSet(
    'LIKES', ['Person'], ['Movie'], ['name'], ['title']
)
```

In this example, `people` is a `NodeSet` with the label `Person` and a unique property `name`,
`movies` is a `NodeSet` with the label `Movie` and a unique property `title`.

Note that both labels and merge keys are lists, so you can define multiple labels and merge keys.

`person_likes_movie` is a `RelationshipSet` with the type `LIKES`, start nodes with label `Person`,
end nodes with label `Movie`, and the properties `name` and `title` to match the start and end nodes 
(equivalent to the `merge_keys` of the respective nodes).

#### Add data to datasets
Data can now be added to these sets and loaded into Neo4j.

```python
people.add_node({'name': 'Alice'})
movies.add_node({'title': 'Matrix'})
person_likes_movie.add_relationship({'name': 'Alice'}, {'title': 'Matrix'})
```

To add a node to the node set, use the `add_node` method with a dictionary of properties. The properties must
contain the unique properties defined in the `merge_keys`.

To add a relationship to the relationship set, use the `add_relationship` method 
with dictionaries containing the specified properties to match the start and end nodes.

#### Create indexes
Before loading data we should create indexes for the merge keys.

```python
people.create_index(driver)
movies.create_index(driver)
``` 

#### Load to Neo4j
Finally, the data can be loaded into Neo4j.

```python
people.create(driver)
movies.create(driver)
person_likes_movie.create(driver)
```

Graphio takes care of batching and efficiently loads the data into Neo4j.

Next to `.create()` Graphio also offers a `.merge()` operation on `NodeSet` and `RelationshipSet`.

!!! Note
    Graphio does not check if the source or targe nodes exist before creating relationships.
    If the nodes do not exist, they will not be created.


