from collections import namedtuple
import logging
from py2neo import Graph, Node, Relationship
from py2neo.matching import *
from typing import Type, Union

from graphio.objects.nodeset import NodeSet
from graphio.objects.relationshipset import RelationshipSet

log = logging.getLogger(__name__)


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

    def __init__(self, *args, **kwargs):
        self.properties = {}
        for k, v in kwargs.items():
            self.properties[k] = v

        self._node = Node(*self.__class__.__labels__, **self.merge_props, **self.additional_props)

    @classmethod
    def dataset(cls):
        return NodeSet(cls.__labels__, merge_keys=cls.__merge_keys__)

    @property
    def merge_props(self) -> dict:
        merge_props = {}
        for k in self.__class__.__merge_keys__:
            try:
                merge_props[k] = self.properties[k]
            except KeyError:
                raise TypeError("Trying to merge node where values for merge_keys are not defined.")
        return merge_props

    @property
    def additional_props(self) -> dict:
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
            raise TypeError(f"Found more than 1 node with properties, not sure what to do: {self.__class__.__labels__}, {self.merge_props}")

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

    def merge(self, graph: Graph):
        if not self.exists(graph):
            graph.create(self._node)

    def link(self, graph: Graph, reltype: Type['ModelRelationship'], target: 'ModelNode', **properties):
        rel = reltype(self, target, **properties)
        rel.merge(graph)


class ModelRelationship:
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
        if not self.exists(graph):
            graph.create(self._relationship)

    @classmethod
    def dataset(cls) -> RelationshipSet:
        return RelationshipSet(cls.type, cls.source.__labels__, cls.target.__labels__, cls.source.__merge_keys__,
                               cls.target.__merge_keys__)
