Use the query generator
==============================

For most graph operations, graphio builds custom Cypher queries and uses py2neo to run them. The query generator functions
can be used in other contexts independent of graphio.

The functions are located in the :mod:`graphio.queries`:

    from graphio.queries import nodes_create_unwind

Create nodes in batches
--------------------------

A Cypher query to :code:`CREATE` nodes with :code:`UNWIND`. Works with multiple labels. The nodes are passed as a parameter (list of node properties)::

    from graphio.queries import nodes_create_unwind

    query = nodes_create_unwind(['Node', 'Label'])

    # run with a py2neo Graph instance
    graph.run(query, props=[{'key': 'value1'}, {'key': 'value2'}]


.. autofunction:: graphio.queries.nodes_create_unwind


Merge nodes in batches
--------------------------

A Cypher query to :code:`MERGE` nodes with :code:`UNWIND`. Works with multiple labels and can :code:`MERGE` on multiple properties.
The nodes are passed as a parameter (list of node properties)::

    from graphio.queries import nodes_merge_unwind

    query = nodes_merge_unwind(['Node', 'Label'], ['node_id'])

    # run with a py2neo Graph instance
    graph.run(query, props=[{'node_id': 'value1'}, {'node_id': 'value2'}])


.. autofunction:: graphio.queries.nodes_merge_unwind
