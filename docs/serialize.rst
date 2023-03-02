==================
Serialization
==================

Graphio can serialize :class:`~graphio.NodeSet` and :class:`~graphio.RelationshipSet` objects to different formats.
This can be used to store processed, graph-ready data in a file.

Graphio supports the following formats for both :class:`~graphio.NodeSet` and :class:`~graphio.RelationshipSet` objects:

- combined CSV and JSON files (CSV file with all data and JSON file with metadata), can be deserialized again
- CSV files with all data (useful for quick tests, cannot be fully deserialized again)
- JSON files with all data (useful for quick tests with small datasets, contains redundant data)


Combined CSV and JSON files
===========================
The most useful serialization format stores the data in a CSV file and the metadata in a JSON file. This avoids
redundancy and allows to deserialize the data again.

Data Format
-----------

Nodes
~~~~~

The JSON file with metadata contains at least the following information:

- the labels (`labels`)
- property keys used for *MERGE* operations (`merge_keys`)

The csv file contains the properties of one node per row, the header contains the property keys.

**Example**:

:code:`nodeset.json`::

    {
        "labels": [
            "Person"
        ],
        "merge_keys": [
            "name"
        ]
    }


:code:`nodeset.csv`::

    name,age
    Lisa,42
    Bob,23

Relationships
~~~~~~~~~~~~~

The JSON file with metadata contains at least the following information:

- start node labels
- end node labels
- start node property keys to `MATCH` the start node
- end node property keys to `MATCH` the end node
- relationship type

The csv file contains one relationship per row, the start node, end node, and relationship properties are indicated
by header prefixes (`start_`, `end_`, `rel_`).

**Example**:

:code:`relset.json`::


    {
      "start_node_labels": ["Person"],
      "end_node_labels": ["Person"],
      "start_node_properties": ["name"],
      "end_node_properties": ["name"],
      "rel_type": "KNOWS"
    }

:code:`relset.csv`::


    start_name,end_name,rel_since
    Lisa,Bob,2018
    Bob,Lisa,2018

Serialize to CSV and JSON
-------------------------

To serialize a :class:`~graphio.NodeSet` or :class:`~graphio.RelationshipSet` object use :func:`to_csv_json_set()`::

  people = NodeSet(['Person'], merge_keys=['name']

  people.add_node({'name': 'Lisa'})
  people.add_node({'name': 'Bob'})

  people.to_csv_json_set('people.json', 'people.csv')

  knows = RelationshipSet('KNOWS', ['Person'], ['Person'], ['name'], ['name'])
  knows.add_relationship({'name': 'Lisa'}, {'name': 'Bob'}, {'since': '2018'})

  knows.to_csv_json_set('knows.json', 'knows.csv')


CSV files
=========

Graphio can serialize :class:`~graphio.NodeSet` and :class:`~graphio.RelationshipSet` objects to CSV files in the same
format as the CSV files in the combined CSV/JSON format. This can be useful for quick tests with small datasets.

See :func:`NodeSet.to_csv()` and :func:`RelationshipSet.to_csv()` for details::

  people = NodeSet(['Person'], merge_keys=['name']

  people.add_node({'name': 'Lisa'})
  people.add_node({'name': 'Bob'})

  people.to_csv('nodeset.csv')

  knows = RelationshipSet('KNOWS', ['Person'], ['Person'], ['name'], ['name'])
  knows.add_relationship({'name': 'Lisa'}, {'name': 'Bob'}, {'since': '2018'})

  knows.to_csv('relset.csv')

Graphio can generate matching Cypher queries to load these CSV files to Neo4j::

  # NodeSet CREATE query
  people.create_csv_query('nodeset.csv')

  # NodeSet MERGE query
  people.merge_csv_query('nodeset.csv')

  # RelationshipSet CREATE query
  knows.create_csv_query('relset.csv')


JSON files
==========
:note: Deserialization of simple JSON representations is currently not supported. Use the combined JSON/CSV format instead.
        The JSON serialization can still be useful to test small datasets.

:class:`~graphio.NodeSet` and :class:`~graphio.RelationshipSet` objects can be serialized to JSON::

   people = NodeSet(['Person'], merge_keys=['name']

   people.add_node({'name': 'Lisa'})

   people.to_json('nodeset.json')


This will create a JSON file with full node descriptions:

:code:`nodeset.json`::

  {
    "labels": [
        "Person"
    ],
    "merge_keys": [
        "name"
    ],
    "nodes": [
        {
            "name": "Lisa"
        }
    ]
  }


The same works with :class:`~graphio.RelationshipSet` objects::

  person_like_food = RelationshipSet('LIKES', ['Person'], ['Food'], ['name'], ['type'])

  person_like_food.add_relationship({'name': 'Lisa'}, {'type': 'Sushi'}, {'since': 'always'})

  person_like_food.to_json('relset.json')



