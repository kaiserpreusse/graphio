from collections import namedtuple

from graphio.objects.nodeset import NodeSet
from graphio.objects.relationshipset import RelationshipSet


class StringContainer:
    def __init__(self, v):
        self.v = v

    def __repr__(self):
        return self.v

    def __str__(self):
        return self.v

    def __eq__(self, other):
        return str(self) == other


class MergeKey(StringContainer):
    def __init__(self, v):
        super(MergeKey, self).__init__(v)


class Label(StringContainer):
    def __init__(self, v):
        super(Label, self).__init__(v)


class MetaNode(type):

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

    @classmethod
    def dataset(cls):
        return NodeSet(cls.__labels__, merge_keys=cls.__merge_keys__)


class ModelRelationship():
    source = None
    target = None
    type = ''

    @classmethod
    def dataset(cls):
        return RelationshipSet(cls.type, cls.source.__labels__, cls.target.__labels__, cls.source.__merge_keys__,
                               cls.target.__merge_keys__)
