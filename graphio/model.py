from collections import namedtuple
import logging
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


    @classmethod
    def dataset(cls) -> RelationshipSet:
        """
        :return: Return a :class:`~graphio.RelationshipSet` instance for this ModelRelationship.
        """
        return RelationshipSet(cls.type, cls.source.__labels__, cls.target.__labels__, cls.source.__merge_keys__,
                               cls.target.__merge_keys__)
