.. graphio documentation master file, created by
   sphinx-quickstart on Tue Dec 31 16:46:22 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

graphio documentation
===================================

Graphio is a Python library to quickly load data to Neo4j. The goal of graphio is to collect
multiple sets of nodes and relationships from files or other data sources and write them to Neo4j. Graphio only writes
data, it is not meant for querying Neo4j and returning data. A common task is to parse an Excel file that contains condensed
information. Typical cases are multiple nodes per row or rows containing both nodes and relationship.

Graphio focuses on creating :class:`graphio.NodeSet` and :class:`graphio.RelationshipSet` which are groups of nodes
and relationships with similiar properties. Graphio can write these data sets to Neo4j using :code:`CREATE` or :code:`MERGE` operations.

Graphio is based on py2neo which is used to run queries. While py2neo is a comprehensive Neo4j library including object-graph mapping,
graphio is made to quickly build a Neo4j database from existing data sets.


Example
-----------
Iterate over a file that contains data for nodes and create a :class:`graphio.NodeSet`::

   # under the hood py2neo is used to connect to Neo4j
   # you always need a py2neo.Graph instance
   from py2neo import Graph
   graph = Graph()

   from graphio import NodeSet

   # define a NodeSet with labels and merge_keys
   nodeset = NodeSet(['Person'], merge_keys=['name'])

   with open('people.tsv') as my_file:
      for line in my_file:
         name = line.strip()
         nodeset.add_node({'name': name})

   # create the nodes in NodeSet, needs a py2neo.Graph instance
   nodeset.create(graph)





Prerequisites
----------------


.. toctree::
   :maxdepth: 2

   nodesets
   relsets




Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
