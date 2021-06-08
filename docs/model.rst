==================
Model Objects
==================

.. warning::
    This is the first iteration of the interface for model objects. The function/class signatures might
    change in the next releases.

Basic Usage
-------------

Creating :class:`~graphio.NodeSet` and :class:`~graphio.RelationshipSet` classes with string is error prone.

Graphio offers a simple object graph model system::

  from graphio import ModelNode, ModelRelationship

  class Person(ModelNode):
      name = MergeKey()

  class Food(ModelNode):
      type = MergeKey()

  class PersonLikes(ModelRelationship):
      source = Person
      target = Food
      type = 'LIKES'

You can use these classes to create :class:`~graphio.NodeSet` and :class:`~graphio.RelationshipSet`::

  person_nodeset = Person.dataset()
  food_nodeset = Food.dataset()

  person_likes_food = PersonLikes.dataset()

When adding data to the :class:`~graphio.RelationshipSet` you can use the :code:`MergeKey` properties of the
:class:`ModelNode` classes to avoid typing the properties as strings::

  for name, food in [('Susan', 'Pizza'), ('Ann', 'Sushi')]:
      person_likes_food.add_relationship(
          {Person.name: name}, {Food.type: food}
      )

You can set one or multiple :code:`Label` and :code:`MergeKey` properties on the :class:`ModelNode`::

   class Person(ModelNode):
       first_name = MergeKey()
       last_name = MergeKey()

       Person = Label()
       Human = Label()

You can override the actual values of the :code:`Label` and :code:`MergeKey`::

   class Person(ModelNode):
       first_name = MergeKey('first_name')
       last_name = MergeKey('surname')

       Person = Label('Individual')
       Human = Label('HomoSapiens')


Add data with model instances
----------------------------------
You can create instances of the model objects to create individual nodes and relationships::

  from graphio import ModelNode, ModelRelationship
  from py2neo import Graph

  graph = Graph()

  class Person(ModelNode):
      name = MergeKey()

  class Food(ModelNode):
      type = MergeKey()

  class PersonLikes(ModelRelationship):
      source = Person
      target = Food
      type = 'LIKES'

  alice = Person(name='Alice')
  sushi = Food(type='Sushi')

  alice.merge(graph)
  sushi.merge(graph)

  alice_likes_sushi = PersonLikes(alice, sushi)
  alice_likes_sushi.merge(graph)

You can also link nodes without creating :class:`~graphio.ModelRelationship` instances::

  alice.link(graph, PersonLikes, sushi, since='always')


ModelNode
-------------

.. autoclass:: graphio.ModelNode
    :members:

ModelRelationship
-------------

.. autoclass:: graphio.ModelRelationship
    :members:

Helper Classes
---------------
.. autoclass:: graphio.NodeDescriptor
    :members: