==================
Serialization
==================

:class:`~graphio.NodeSet` and :class:`~graphio.RelationshipSet` objects can be serialized to JSON::

   people = NodeSet(['Person'], merge_keys=['name']

   people.add_node({'name': 'Lisa'})

   people.serialize('/path/to/target')

This will create a JSON file with a filename containing the labels, merge_keys and a unique ID:

:code:`nodeset_Person_name_19257168-e29b-484f-b96d-5a2d03e60707.json`::

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

To deserialize use :func:`NodeSet.from_dict()`::

    with open('nodeset_Person_name_19257168-e29b-484f-b96d-5a2d03e60707.json', 'rt') as f:
        ns = NodeSet.from_dict(json.load(f))



Under the hood :func:`serialize()` uses :func:`NodeSet.to_dict()`. Call that function directly to set a filename::

  with open('/path/to/my_nodeset.json', 'wt') as f:
      json.dump(people.to_dict(), f)

The same works with :class:`~graphio.RelationshipSet` objects::

  person_like_food = RelationshipSet('LIKES', ['Person'], ['Food'], ['name'], ['type'])

  person_like_food.add_relationship({'name': 'Lisa'}, {'type': 'Sushi'}, {'since': 'always'})

  person_like_food.serialize('/path/to/target')

