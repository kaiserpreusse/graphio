.. graphio documentation master file, created by
   sphinx-quickstart on Tue Dec 31 16:46:22 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

graphio documentation
===================================

Graphio is a Python library to bulk load data to Neo4j. The goal of graphio is to collect
multiple sets of nodes and relationships from files or other data sources and write them to Neo4j. Graphio only writes
data, it is not meant for querying Neo4j and returning data. A common task is to parse a bunch of Excel files to create
a Neo4j prototype.

Graphio focuses on creating :class:`~graphio.NodeSet` and :class:`~graphio.RelationshipSet` which are groups of nodes
and relationships with similiar properties. Graphio can write these data sets to Neo4j using :code:`CREATE` or :code:`MERGE` operations.

Graphio uses py2neo in several places. Part of the bulk data loading logic developed for graphio was merged into py2neo.
While py2neo is a comprehensive Neo4j library including object-graph mapping, graphio is made to quickly build a Neo4j database from existing data sets.


Version
-----------

.. image:: https://img.shields.io/pypi/v/graphio
   :target: https://pypi.org/project/graphio


Install
++++++++++++++++++

Use pip to install::

  pip install -U graphio


Example
-----------
Iterate over a file that contains people and the movies they like and extract nodes and relationships.
This is an example for a self-defined data file that you have to parse. Contents of example file 'people.tsv'::

   Alice; Matrix,Titanic
   Peter; Matrix,Forrest Gump
   John; Forrest Gump,Titanic

The goal is to create the follwing data in Neo4j:

- :code:`(Person)` nodes
- :code:`(Movie)` nodes
- :code:`(Person)-[:LIKES]->(Movie)` relationships

::

   # under the hood py2neo is used to connect to Neo4j
   # you always need a py2neo.Graph instance
   from py2neo import Graph
   graph = Graph()

   from graphio import NodeSet, RelationshipSet

   # define data sets
   people = NodeSet(['Person'], merge_keys=['name'])
   movies = NodeSet(['Movie'], merge_keys=['title'])
   person_likes_movie = RelationshipSet('LIKES', ['Person'], ['Movie'], ['name'], ['title'])

   with open('people.tsv') as my_file:
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
            person_likes_movie.add_relationship({'name': name}, {'title': title}, {'source': 'my_file'})


   # create the nodes in NodeSet, needs a py2neo.Graph instance
   people.create(graph)
   movies.create(graph)
   person_likes_movie.create(graph)


The code in the example should be easy to understand:

1. Define the data sets you want to add.
2. Iterate over a data source, transform the data and add to the data sets.
3. Store data in Neo4j.

.. note::
   The example does create mulitple nodes with the same properties. You have to take care of uniqueness yourself.

Create and Merge operations
--------------------------

Graphio can run both :code:`create()` and :code:`merge()` operations on :code:`NodeSet` and :code:`RelationshipSet`.

See details in the documentation of  :class:`~graphio.NodeSet` and :class:`~graphio.RelationshipSet`.

Create
++++++++
:code:`create()` will, as the name suggests, create all data. This will create
duplicate nodes even if a :code:`merge_key` is set on a :code:`NodeSet`.

Merge
++++++++
:code:`merge()` will merge on the :code:`merge_key` defined on the :code:`NodeSet`.

To control the merge operation, you can pass a list of properties that should not be overwritten on existing nodes::

  NodeSet.merge(graph, preserve=['name', 'currency'])

This is equivalent to::

  ON CREATE SET ..all properties..
  ON MATCH SET ..all properties except 'name' and 'currency'


Graphio can also append properties to arrays::

  NodeSet.merge(graph, append_props=['source'])

This will create a list for the node property :code:`source` and append values :code:`ON MATCH`.

Both can also be set on the :code:`NodeSet`::

  nodeset = NodeSet(['Person'], ['name'], preserve=['country'], array_props=['source'])



Contents
----------------


.. toctree::
   :maxdepth: 3

   getting_started
   objects
   queries




Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
