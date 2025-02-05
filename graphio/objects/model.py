from typing import List, Union

from graphio import NodeSet, RelationshipSet


class RegistryMeta(type):
    def __init__(cls, name, bases, attrs):
        if not hasattr(cls, 'registry'):
            cls.registry = []
        if cls not in cls.registry:
            if cls.__name__ != 'NodeModel' and cls.__name__ != 'RelationshipModel':
                cls.registry.append(cls)
        super().__init__(name, bases, attrs)


class NodeModel(metaclass=RegistryMeta):
    """
    Entrypoint for the application.
    """
    labels: List[str]
    merge_keys: List[str]
    default_props: dict = None
    preserve: List[str] = None
    append_props: List[str] = None
    additional_labels: List[str] = None

    @classmethod
    def get_class_by_name(cls, name):
        for subclass in cls.registry:
            if subclass.__name__ == name:
                return subclass
        return None

    @classmethod
    def nodeset(cls):
        """
        Create a NodeSet from this Node.

        :return: NodeSet
        """
        labels = cls.labels
        merge_keys = cls.merge_keys
        default_props = cls.default_props
        preserve = cls.preserve
        append_props = cls.append_props
        additional_labels = cls.additional_labels

        return NodeSet(labels=labels, merge_keys=merge_keys, default_props=default_props,
                       preserve=preserve, append_props=append_props, additional_labels=additional_labels)

    @classmethod
    def dataset(cls):
        return cls.nodeset()

    @classmethod
    def create_index(cls, driver):
        cls.nodeset().create_index(driver)


class Relationship:

    def __init__(self, source: str, rel_type: str, target: str):
        self.rel_type = rel_type
        self.source = source
        self.target = target

    def dataset(self):
        source_node = NodeModel.get_class_by_name(self.source)
        target_node = NodeModel.get_class_by_name(self.target)

        return RelationshipSet(
            rel_type=self.rel_type,
            start_node_labels=source_node.labels,
            end_node_labels=target_node.labels,
            start_node_properties=source_node.merge_keys,
            end_node_properties=target_node.merge_keys
        )

    def relationshipset(self):
        return self.dataset()

    def set(self):
        return self.dataset()


class RelationshipModel(metaclass=RegistryMeta):
    rel_type: str
    source: type[NodeModel]
    target: type[NodeModel]
    default_props: dict = None

    @classmethod
    def get_class_by_name(cls, name):
        for subclass in cls.registry:
            if subclass.__name__ == name:
                return subclass
        return None

    @classmethod
    def relationshipset(cls):
        return RelationshipSet(
            rel_type=cls.rel_type,
            start_node_labels=cls.source.labels,
            end_node_labels=cls.target.labels,
            start_node_properties=cls.source.merge_keys,
            end_node_properties=cls.target.merge_keys,
            default_props=cls.default_props
        )

    @classmethod
    def dataset(self):
        return self.relationshipset()

    @classmethod
    def create_index(cls, driver):
        cls.relationshipset().create_index(driver)


class GraphModel:
    """
    Container for NodeModel and RelationshipModel classes.
    """

    nodes: List[NodeModel] = []
    relationships: List[RelationshipModel] = []

    def add(self, object):
        if issubclass(object, NodeModel):
            self.nodes.append(object)
        elif issubclass(object, RelationshipModel):
            self.relationships.append(object)

    def create_indexes(self, driver):
        for node in self.nodes:
            node.create_index(driver)
        for rel in self.relationships:
            rel.create_index(driver)


class Container:
    """
    A container for a collection of Nodes, Relationships, NodeSets and RelationshipSets.

    A typical parser function to e.g. read an Excel file produces a mixed output which then has to
    be processed accordingly.

    Also, sanity checks and data statistics are useful.
    """

    def __init__(self, objects=None):
        self.objects = []

        # add objects if they are passed
        if objects:
            for o in objects:
                self.objects.append(o)

    @property
    def nodesets(self):
        """
        Get the NodeSets in the Container.
        """
        return get_instances_from_list(self.objects, NodeSet)

    @property
    def relationshipsets(self):
        """
        Get the RelationshipSets in the Container.
        """
        return get_instances_from_list(self.objects, RelationshipSet)

    def get_nodeset(self, labels, merge_keys):
        for nodeset in self.nodesets:
            if set(nodeset.labels) == set(labels) and set(nodeset.merge_keys) == set(merge_keys):
                return nodeset

    def add(self, object):
        self.objects.append(object)

    def add_all(self, objects):
        for o in objects:
            self.add(o)

    def merge_nodesets(self):
        """
        Merge all node sets if merge_key is defined.
        """
        for nodeset in self.nodesets:
            nodeset.merge(nodeset.merge_keys)

    def create_relationshipsets(self):
        for relationshipset in self.relationshipsets:
            relationshipset.create()


def get_instances_from_list(list, klass):
    """
    From a list of objects, get all objects that are instance of klass.

    :param list: List of objects.
    :param klass: The reference class.
    :return: Filtered list if objects.
    """
    output = []
    for o in list:
        if isinstance(o, klass):
            output.append(o)
    return output
