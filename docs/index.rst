.. graphio documentation master file, created by
   sphinx-quickstart on Tue Dec 31 16:46:22 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

graphio documentation
===================================

Graphio is a Python library for bulk loading data to Neo4j. Graphio collects
multiple sets of nodes and relationships and loads them to Neo4j. A common example is parsing a set of Excel files
to create a Neo4j prototype. Graphio only loads data, it is not meant for querying Neo4j and returning data.

Graphio can serialize data to JSON and CSV files. This is useful for debugging and for storing graph ready data sets.

The primary interface are :class:`~graphio.NodeSet` and :class:`~graphio.RelationshipSet` classes which are groups of nodes
and relationships with similiar properties. Graphio can load these data sets to Neo4j using :code:`CREATE` or :code:`MERGE` operations.

Graphio uses the official `Neo4j Python driver <https://neo4j.com/docs/api/python-driver/current/>`_ to connect to Neo4j.

..  warning:: Graphio was initially built on top of `py2neo <https://py2neo.org/2021.1/>`_ which is not actively maintained
   anymore. The most recent version of py2neo still works with graphio but this is not supported anymore. Please switch
   to the official Neo4j Python driver.




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
Iterate over a file that contains people and the movies they like and extract nodes and relationships. Contents of example file 'people.tsv'::

   Alice; Matrix,Titanic
   Peter; Matrix,Forrest Gump
   John; Forrest Gump,Titanic

The goal is to create the follwing data in Neo4j:

- :code:`(Person)` nodes
- :code:`(Movie)` nodes
- :code:`(Person)-[:LIKES]->(Movie)` relationships

::

   # the official Neo4j driver is used to connect to Neo4j
   # you always need a Driver instance
   from neo4j import GraphDatabase

   driver = GraphDatabase.driver('neo4j://localhost:7687', auth=('neo4j', 'password'))

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
   people.create(driver)
   movies.create(driver)
   person_likes_movie.create(driver)


The code in the example should be easy to understand:

1. Define the data sets you want to add.
2. Iterate over a data source, transform the data and add to the data sets.
3. Store data in Neo4j.

.. note::
   The example does create mulitple nodes with the same properties. You have to take care of uniqueness yourself.


Continue with the :doc:`Basic Workflow section <basic_workflow>`.


Contents
----------------


.. toctree::
   :maxdepth: 2

   basic_workflow
   serialize
   objects
   model




Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
