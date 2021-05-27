==================
Basic Workflow
==================

NodeSets
-----------

With graphio you predefine the :class:`~graphio.NodeSet` and add nodes::

   from graphio import NodeSet

   people = NodeSet(['Person'], merge_keys=['name'])

   people.add_node({'name': 'Peter', 'city': 'Munich'})

The first argument for the :class:`~graphio.NodeSet` is a list of labels used for all nodes in this :class:`~graphio.NodeSet`.
The second optional argument are :attr:`merge_keys`, a list of properties that confer uniqueness of the nodes
in this :class:`~graphio.NodeSet`. All operations
based on :code:`MERGE` queries need unique properties to identify nodes.

When you add a node to the NodeSet you can add arbitrary properties to the node.

Uniqueness of nodes
+++++++++++++++++++++

The uniqueness of the nodes is not checked when adding to the NodeSet. Thus, you can create mulitple nodes with the same 'name' property.

Use :code:`NodeSet.add_unique()` to check if a node with the same properties exist already::

  people = NodeSet(['Person'], merge_keys=['name'])

  # first time
  people.add_unique({'name': 'Jack', 'city': 'London'})
  len(people.nodes) -> 1

  # second time
  people.add_unique({'name': 'Jack', 'city': 'London'})
  len(people.nodes) -> 1


.. warning::
  This function iterates all nodes when adding a new one and does not scale well. Use only for small nodesets.


Default properties
+++++++++++++++++++

You can set default properties on the :class:`~graphio.NodeSet` that are added to all nodes when loading data::

  people_in_europe = NodeSet(['Person'], merge_keys=['name'],
                             default_props={'continent': 'Europe'})


RelationshipSets
-----------------

In a similar manner, :class:`~graphio.RelationshipSet` are predefined and you add relationships::

   from graphio import RelationshipSet

   person_likes_food = RelationshipSet('KNOWS', ['Person'], ['Food'], ['name'], ['type'])

   person_likes_food.add_relationship(
      {'name': 'Peter'}, {'type': 'Pizza'}, {'reason': 'cheese'}
   )

The arguments for the :class:`~graphio.RelationshipSet`

- relationship type
- labels of start node
- labels of end node
- property keys to match start node
- property keys to match end node

When you add a relationship to :class:`~graphio.RelationshipSet` all you have to do is to define the matching properties for the
start node and end node. You can also add relationship properties.

Default properties
+++++++++++++++++++

You can set default properties on the :class:`~graphio.RelationshipSet` that are added to all relationships when loading data::

  person_likes_food = RelationshipSet('KNOWS', ['Person'], ['Food'], ['name'], ['type'],
                                      default_props={'source': 'survey'})

Create Indexes
---------------

Both class:`~graphio.NodeSet` and :class:`~graphio.RelationshipSet` allow you to create indexes to speed up data loading.
:func:`NodeSet.create_index()` creates indexes for all individual :code:`merge_keys` properties as well as a compound index.
:func:`RelationshipSet.create_index()` creates the indexes required for matching the start node and end node::

  from graphio import RelationshipSet
  from py2neo import Graph

  graph = Graph()

  person_likes_food = RelationshipSet('KNOWS', ['Person'], ['Food'], ['name'], ['type'])

  person_likes_food.create_index(graph)

This will create single-property indexes for `:Person(name)` and `:Food(type)`.

Load Data
---------------

After building :class:`~graphio.NodeSet` and :class:`~graphio.RelationshipSet` you can create or merge everything in Neo4j.

You need a :class:`py2neo.Graph` instance to create data. See: https://py2neo.org/v4/database.html#the-graph

::

    from py2neo import Graph

    graph = Graph()

    people.create(graph)
    person_likes_food.create(graph)

.. warning::
    Graphio does not check if the nodes referenced in the :class:`~graphio.RelationshipSet` actually exist. It is meant
    to quickly build data sets and throw them into Neo4j, not to maintain consistency.


Create
++++++++
:code:`create()` will, as the name suggests, create all data. This will create
duplicate nodes even if a :code:`merge_key` is set on a :code:`NodeSet`.

Merge
++++++++
:func:`merge()` will merge on the :code:`merge_key` defined on the :code:`NodeSet`.

The merge operation for :class:`~graphio.NodeSet` offers more control.

You can pass a list of properties that should not be overwritten on existing nodes::

  NodeSet.merge(graph, preserve=['name', 'currency'])

This is equivalent to::

  ON CREATE SET ..all properties..
  ON MATCH SET ..all properties except 'name' and 'currency'..


Graphio can also append properties to arrays::

  NodeSet.merge(graph, append_props=['source'])

This will create a list for the node property :code:`source` and append values :code:`ON MATCH`.

Both can also be set on the :code:`NodeSet`::

  nodeset = NodeSet(['Person'], ['name'], preserve=['country'], array_props=['source'])




Group Data Sets in a Container
--------------------------------
A :class:`~graphio.Container` can be used to group :class:`~graphio.NodeSet` and :class:`~graphio.RelationshipSet`::

    my_data = Container()

    my_data.add(people)
    my_data.add(person_likes_food)

.. note::
    This is particularly useful if you build many :class:`~graphio.NodeSet` and :class:`~graphio.RelationshipSet`
    and want to group data sets (e.g. because of dependencies).

You can iterate the :class:`~graphio.NodeSet` and :class:`~graphio.RelationshipSet` in the :class:`~graphio.Container`::

    for nodeset in my_data.nodesets:
        nodeset.create(graph)

