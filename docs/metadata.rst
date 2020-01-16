Store Data Sets and Metadata
==============================

In most data projects you have metadata associated to your data sets. To make it easer to store metadata, all objects
on Graphio can be stored as nodes in Neo4j. Graphio uses the :mod:`py2neo.ogm` object-graph mapper module to facilitate this.

Both :class:`~grahio.NodeSet` and :class:`~graphio.RelationshipSet` inherit from :class:`py2neo.ogm.GraphObject`. As such, they
are objects that are mapped to Neo4j nodes through :attr:`py2neo`.

However, you have to define the node properties you want to use in your project as well as potential relationships to other
classes you use. See the :mod:`py2neo.ogm` docs for a full introduction: https://py2neo.org/v4/ogm.html

Subclass NodeSet and RelationshipSet
--------------------------------------

The easiest way is to **subclass** :class:`~grahio.NodeSet` and :class:`~graphio.RelationshipSet` to add mapped properties
and relationships::

    from graphio import NodeSet

    # inherit from graphio.NodeSet
    class MyNodeSet(NodeSet):
        # define the label and primary key (see py2neo docs)
        __primarylabel__ = 'NodeSet'
        __primarykey__ = 'uuid'

        labels = Property()
        merge_keys = Property()

        uuid = Property()
        date = Property()

        def __init__(self, labels, merge_keys, uuid, date):

            super(MyNodeSet, self).__init__(labels, merge_keys)

            self.uuid = uuid
            self.date = date

Works in a similar fashion with RelationshipSet::

    from graphio import RelationshipSet

    # inherit from graphio.RelationshipSet
    class MyRelSet(RelationshipSet):
        # define the label and primary key (see py2neo docs)
        __primarylabel__ = 'RelationshipSet'
        __primarykey__ = 'combined'

        uuid = Property()
        rel_type = Property()
        start_node_labels = Property()
        end_node_labels = Property()
        start_node_properties = Property()
        end_node_properties = Property()

        def __init__(self, rel_type, start_node_labels, end_node_labels, start_node_properties, end_node_properties, uuid, date):

            super(MyRelSet, self).__init__(rel_type, start_node_labels, end_node_labels, start_node_properties, end_node_properties)

            self.uuid = uuid
            self.date = date


Store Objects in Neo4j
-----------------------

Now you can store the :class:`~grahio.NodeSet` and :class:`~graphio.RelationshipSet` nodes in Neo4j::

    from py2neo import Graph

    graph = Graph()

    mynodeset = MyNodeSet(['Person'], ['name'], 'some_id', 'today')

    graph.push(mynnodeset)

This will create a node with all properties defined in :class:`MyNodeSet`

.. note::
    **All** properties for the node have to be defined in the new subclass (even :attr:`labels`, :attr:`merge_keys` etc).
    This gives the user more flexibility to handle the data stored on the nodes.
