from collections import namedtuple
import logging
from py2neo import Graph, Node, Relationship
from py2neo.matching import *
from typing import Type, Union, List, Optional
from dataclasses import dataclass

from graphio.objects.nodeset import NodeSet
from graphio.objects.relationshipset import RelationshipSet

log = logging.getLogger(__name__)


class NodeDescriptor:
    """
    Unified interface to describe nodes with labels and properties.

    `NodeDescriptor` instances are passed into functions when no `ModelNode` classes or instances are available.

    Setting `merge_keys` is optional. If they are not set all property keys will be used as merge_keys.
    """

    def __init__(self, labels: List[str], properties: dict, merge_keys: List[str] = None):
        """

        :param labels: Labels for this node.
        :param properties: Properties for this node.
        :param merge_keys: Optional.
        """
        self.labels = labels
        self.properties = properties

        if merge_keys:
            self.merge_keys = merge_keys
        else:
            self.merge_keys = list(self.properties.keys())

    def get_modelnode(self) -> 'ModelNode':
        NodeClass = ModelNode.factory(self.labels, merge_keys=self.merge_keys)
        node_instance = NodeClass(**self.properties)
        return node_instance


class StringContainer:
    def __init__(self, v: str = None):
        self.v = v

    def __repr__(self):
        return self.v

    def __str__(self):
        return self.v

    def __eq__(self, other):
        return str(self) == other


class MergeKey(StringContainer):
    def __init__(self, v: str = None):
        super(MergeKey, self).__init__(v)


class Label(StringContainer):
    def __init__(self, v: str = None):
        super(Label, self).__init__(v)


class MetaNode(type):

    def __init__(cls, *args, **kwargs):
        # set value on empty StringContainer
        for key, value in cls.__dict__.items():
            if isinstance(value, StringContainer):
                if not value.v:
                    value.v = key

        # add Label(ClassName) if none is given
        if not cls.__labels__:
            setattr(cls, cls.__name__, Label(cls.__name__))

    @property
    def __merge_keys__(cls):
        merge_keys = []
        for v in cls.__dict__.values():
            if isinstance(v, MergeKey):
                merge_keys.append(str(v))
        return merge_keys

    @property
    def __labels__(cls):
        labels = []
        for v in cls.__dict__.values():
            if isinstance(v, Label):
                labels.append(str(v))
        return labels

    def __getattribute__(cls, item):
        value = super(MetaNode, cls).__getattribute__(item)
        if isinstance(value, StringContainer):
            return str(value)

        else:
            return value


class ModelNode(metaclass=MetaNode):
    """
    Baseclass for model objects.
    """

    def __init__(self, *args, **kwargs):
        self.properties = {}
        for k, v in kwargs.items():
            self.properties[k] = v

        self._node = Node(*self.__class__.__labels__, **self.merge_props, **self.additional_props)

    @classmethod
    def dataset(cls) -> NodeSet:
        """
        :return: Return a :class:`~graphio.NodeSet` instance for this ModelNode.
        """
        return NodeSet(cls.__labels__, merge_keys=cls.__merge_keys__)

    @classmethod
    def factory(cls, labels: List[str], merge_keys: List[str] = None, name: str = None) -> type:
        """
        Create a class with given labels and merge_keys. The merge_keys are optional but some functions do not work
        without them.

        :param labels: Labels for this ModelNode class.
        :param merge_keys: MergeKeys for this ModelNode class.
        :return: The ModelNode class.
        """
        if not name:
            name = 'FactoryModelNode'

        attributes = {}
        for l in labels:
            attributes[l] = Label(l)
        if merge_keys:
            for k in merge_keys:
                attributes[k] = MergeKey(k)

        FactoryModelNode = type(name, (cls,), attributes)
        return FactoryModelNode

    @property
    def merge_props(self) -> dict:
        """
        Return the merge properties for this node.

        :return: Dictionary with the merge properties for this node.
        :rtype: dict
        """
        merge_props = {}
        for k in self.__class__.__merge_keys__:
            try:
                merge_props[k] = self.properties[k]
            except KeyError:
                raise TypeError("Trying to merge node where values for merge_keys are not defined.")
        return merge_props

    @property
    def additional_props(self) -> dict:
        """
        Return all properties except the merge properties.

        :return: Dictionary with all properties except the merge properties.
        :rtype: dict
        """
        additional_props = {}
        for k, v in self.properties.items():
            if k not in self.__class__.__merge_keys__:
                additional_props[k] = v
        return additional_props

    def exists(self, graph: Graph) -> Union[bool, Node]:
        """
        Check if node exists in the graph. If yes: return the Node. If no: return false.

        Raise a TypeError if there is more than one Node found with these properties. Node definition
        only makes sense if working with unique nodes.

        :param graph: py2neo.Graph instance
        :return: The node or False.
        """
        matcher = NodeMatcher(graph)
        node = matcher.match(*self.__class__.__labels__, **self.merge_props)

        if len(node) > 1:
            raise TypeError(
                f"Found more than 1 node with properties, not sure what to do: {self.__class__.__labels__}, {self.merge_props}")

        if node:
            return list(node)[0]
        else:
            return False

    def bind(self, graph):
        """
        Make sure the node is bound to the graph.

        :param graph: py2neo.Graph
        """
        node_or_false = self.exists(graph)
        if node_or_false:
            self._node = node_or_false

    def merge(self, graph: Graph) -> None:
        """
        :code:`MERGE` the node in the graph.

        :param graph: A py2neo.Graph instance. 
        """
        if not self.exists(graph):
            graph.create(self._node)

    def link(self, graph: Graph, reltype: Union[Type['ModelRelationship'], str],
             target: Union['ModelNode', NodeDescriptor], **properties) -> None:
        """
        Link the node to another node.

        Input is either a combination of a :class:`~graphio.ModelRelationship` and :class:`~graphio.ModelNode` or a
        combination of a string for the reltype and a :class:`~graphio.NodeDescriptor` instance to describe the target node.

        :param graph: py2neo.Graph instance.
        :param reltype: Either a :class:`~graphio.ModelRelationship` instance or a string for the relationship type.
        :param target: Either a :class:`~graphio.ModelNode` instance or a :class:`~graphio.NodeDescriptor` instance.
        :param properties: Proeprties for the relationships.
        """
        # get ModelNode if NodeDescriptor is passed
        if isinstance(target, NodeDescriptor):
            target = target.get_modelnode()

        # create reltype if string is passed
        if isinstance(reltype, str):
            reltype = type('LocalRelType', (ModelRelationship,),
                           {'source': self.__class__, 'target': target.__class__, 'type': reltype})

        rel = reltype(self, target, **properties)
        rel.merge(graph)


class ModelRelationship:
    """
    Base class for model relationships.

    Knows about the class of source node and target node (instances of :class:`~graphio.ModelNode`) and the
    relationship type::

      class Person(ModelNode):
          name = MergeKey()

      class Food(ModelNode):
          name = MergeKey()

      class PersonLikesToEat(ModelRelationship):
          source = Person
          target = Food
          type = 'LIKES'

    """
    source = None
    target = None
    type = ''

    def __init__(self, source: 'ModelNode', target: 'ModelNode', **kwargs):
        self.source_node = source
        self.target_node = target
        self.properties = {}
        for k, v in kwargs.items():
            self.properties[k] = v

        self._relationship = Relationship(self.source_node._node, self.type, self.target_node._node, **self.properties)

    def exists(self, graph: Graph) -> bool:
        """
        Check if the relationship exists.

        :param graph: A py2neo.Graph instance.
        :return: True/False
        """
        # return False if either start or end node do not exist
        if not self.source_node.exists(graph) or not self.target_node.exists(graph):
            return False
        else:
            self.source_node.bind(graph)
            self.target_node.bind(graph)

            relmatcher = RelationshipMatcher(graph).match(nodes=(self.source_node._node, self.target_node._node),
                                                          r_type=self.type)
            if len(relmatcher) > 0:
                return True
            else:
                return False

    def merge(self, graph: Graph):
        """
        :code:`MERGE` the relationship in the graph.

        :param graph: A py2neo.Graph instance.
        """
        if not self.exists(graph):
            graph.create(self._relationship)

    @classmethod
    def dataset(cls) -> RelationshipSet:
        """
        :return: Return a :class:`~graphio.RelationshipSet` instance for this ModelRelationship.
        """
        return RelationshipSet(cls.type, cls.source.__labels__, cls.target.__labels__, cls.source.__merge_keys__,
                               cls.target.__merge_keys__)
