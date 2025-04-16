# Data Sets

## NodeSets

`graphio` allows you to define `NodeSet` objects for managing nodes in a graph database.

```python
from graphio import NodeSet

people = NodeSet(['Person'], merge_keys=['name'])
people.add_node({'name': 'Peter', 'city': 'Munich'})
```

### Parameters

- **Labels**: A list of labels applied to all nodes in the `NodeSet`.
- **Merge Keys**: An optional list of properties (`merge_keys`) that define the uniqueness of nodes. These properties are critical for operations using `MERGE` queries.

When adding a node to a `NodeSet`, you can include arbitrary properties.

### Handling Node Uniqueness

Nodes added to a `NodeSet` are not automatically checked for uniqueness. This means you could unintentionally create multiple nodes with the same `merge_key`.

To ensure uniqueness, use the `add_unique()` method:

```python
people = NodeSet(['Person'], merge_keys=['name'])

# First addition
people.add_unique({'name': 'Jack', 'city': 'London'})
len(people.nodes)  # Returns 1

# Second addition (same node)
people.add_unique({'name': 'Jack', 'city': 'London'})
len(people.nodes)  # Still 1
```

!!! warning
    The `add_unique()` method iterates through all nodes to check for duplicates, making it unsuitable for large `NodeSet` instances.

### Default Properties

Default properties can be specified for a `NodeSet`. These properties are automatically applied to all nodes during data loading:

```python
people_in_europe = NodeSet(
    ['Person'], merge_keys=['name'], default_props={'continent': 'Europe'}
)
```

## RelationshipSets

You can define `RelationshipSet` objects to manage relationships in a graph.

```python
from graphio import RelationshipSet

person_likes_food = RelationshipSet(
    'KNOWS', ['Person'], ['Food'], ['name'], ['type']
)

person_likes_food.add_relationship(
    {'name': 'Peter'}, {'type': 'Pizza'}, {'reason': 'cheese'}
)
```

### Parameters

- **Relationship Type**: The type of the relationship, e.g., `KNOWS`.
- **Start and End Node Labels**: Labels for the start and end nodes.
- **Property Keys**: Properties used to match start and end nodes.

You can also add custom properties to relationships.

### Default Properties

Default properties can also be set for `RelationshipSet` instances:

```python
person_likes_food = RelationshipSet(
    'KNOWS', ['Person'], ['Food'], ['name'], ['type'],
    default_props={'source': 'survey'}
)
```

## Creating Indexes

Indexes improve performance when loading data. You can create indexes for `NodeSet` and `RelationshipSet` objects:

```python
from graphio import RelationshipSet
from neo4j import GraphDatabase

driver = GraphDatabase.driver('neo4j://localhost:7687', auth=('neo4j', 'password'))

person_likes_food = RelationshipSet(
    'KNOWS', ['Person'], ['Food'], ['name'], ['type']
)

person_likes_food.create_index(driver)
```

### Index Types

- **NodeSet**: Single-property and compound indexes for `merge_keys`.
- **RelationshipSet**: Indexes for start and end node properties.

Example: The above code creates indexes for `:Person(name)` and `:Food(type)`.

## Loading Data

After defining `NodeSet` and `RelationshipSet` objects, you can load data into Neo4j using a `neo4j.Driver` instance.

```python
from neo4j import GraphDatabase

driver = GraphDatabase.driver('neo4j://localhost:7687', auth=('neo4j', 'password'))

people.create(driver)
person_likes_food.create(driver)
```

!!! warning
    `graphio` does not check if the nodes referenced in a `RelationshipSet` exist. It is designed for quickly building datasets, not ensuring data consistency.

### Create vs. Merge

- **Create**: The `create()` method adds all nodes and relationships without checking for duplicates, even when `merge_keys` are set.
- **Merge**: The `merge()` method uses `merge_keys` to combine existing and new data.

#### Preserving Properties

The `merge()` method can preserve specific properties during the merge process:

```python
NodeSet.merge(driver, preserve=['name', 'currency'])
```

This behavior corresponds to:

- `ON CREATE SET` applies all properties.
- `ON MATCH SET` applies all properties except `name` and `currency`.

#### Appending to Arrays

You can append properties to arrays during a merge:

```python
NodeSet.merge(driver, append_props=['source'])
```

This appends new values to the `source` property.

Both `preserve` and `append_props` can also be set when defining a `NodeSet`:

```python
nodeset = NodeSet(
    ['Person'], ['name'], preserve=['country'], array_props=['source']
)
```
