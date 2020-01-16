==================
Getting Started
==================

NodeSets
-----------

With graphio you predefine the :class:`~graphio.NodeSet` and add nodes::

   from graphio import NodeSet

   people = NodeSet(['Person'], merge_keys=['name'])

   people.add_node({'name': 'Peter', 'city': 'Munich'})

The first argument for the :class:`~graphio.NodeSet` is a list of labels used for all nodes in this :class:`~graphio.NodeSet`. The second optional
argument are :attr:`merge_keys`, a list of properties that confer uniqueness of the nodes in this :class:`~graphio.NodeSet`. All operations
based on :code:`MERGE` queries need unique properties to identify nodes.

When you add a node to the NodeSet you can add arbitrary properties to the node.

.. warning::
   The uniqueness of the nodes is not checked when adding to the NodeSet. Thus, you can create mulitple nodes with
   the same 'name' property. Also, the merge_key property is not required on the node you add.

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


Create Data
---------------

After building :class:`~graphio.NodeSet` and :class:`~graphio.RelationshipSet` you can conveniently create everything in Neo4j.

You need a :class:`py2neo.Graph` instance to create data. See: https://py2neo.org/v4/database.html#the-graph

::

    from py2neo import Graph

    graph = Graph()

    people.create(graph)
    person_likes_food.create(graph)

.. warning::
    Graphio does not check if the nodes referenced in the :class:`~graphio.RelationshipSet` actually exist. It is meant
    to quickly build data sets and throw them into Neo4j, not to maintain consistency.

.. note::
    Right now graphio does not report insert statistics. This is planned for future releases.

.. note::
    Right now graphio does not have functions to sanity check NodeSets and RelationshipSets (i.e. check if the nodes
    referenced in a RelationshipSet actually exist in a given NodeSet). This is planned for future releases.


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

    for nodeset in my_data.nodeset:
        nodeset.create(graph)
